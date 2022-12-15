package server

import (
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"io"
	"net/url"
	"oxia/common"
	"oxia/proto"
	"oxia/server/kv"
	"strconv"
	"sync"
	"time"
)

const (
	KeyPrefix  = common.InternalKeyPrefix + "session/"
	MaxTimeout = 10 * time.Second
)

type SessionId uint32

func Key(sessionId SessionId) string {
	return KeyPrefix + strconv.FormatUint(uint64(sessionId), 16) + "/"
}

func KeyToId(key string) (SessionId, error) {
	s := key[len(KeyPrefix):]
	s = s[:len(s)-1]
	longInt, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, err
	}
	return SessionId(longInt), nil
}

var ErrorSessionNotFound = errors.New("session not found")

var ErrorInvalidSessionTimeout = errors.New("invalid session timeout")

// PutDecorator performs two operations on Put requests that have a session ID.
// First, it checks whether the session is alive. If it is not, it errors out.
// Then it adds an "index" entry as the child of the session key.
var PutDecorator = &putDecorator{}
var versionNotExists int64 = -1

type putDecorator struct{}

func (_ *putDecorator) AdditionalData(putReq *proto.PutRequest) *proto.PutRequest {
	sessionId := putReq.SessionId
	if sessionId == nil {
		return nil
	}
	return &proto.PutRequest{
		Key:             Key(SessionId(*sessionId)) + url.PathEscape(putReq.Key),
		Payload:         []byte{},
		ExpectedVersion: &versionNotExists,
	}
}

func (_ *putDecorator) CheckApplicability(batch kv.WriteBatch, putReq *proto.PutRequest) (proto.Status, error) {
	sessionId := putReq.SessionId
	if sessionId == nil {
		return proto.Status_OK, nil
	}
	var _, closer, err = batch.Get(Key(SessionId(*sessionId)))
	if err != nil {
		if errors.Is(err, kv.ErrorKeyNotFound) {
			return proto.Status_SESSION_DOES_NOT_EXIST, nil

		}
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	if err = closer.Close(); err != nil {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	return proto.Status_OK, nil

}

type LeaderControllerSupplier func(shardId uint32) (LeaderController, error)

// --- SessionManager

type SessionManager interface {
	io.Closer
	UseLeaderControllerSupplier(controllerSupplier LeaderControllerSupplier) SessionManager
	CreateSession(*proto.CreateSessionRequest) (*proto.CreateSessionResponse, error)
	KeepAlive(proto.OxiaClient_KeepAliveServer) error
	CloseSession(*proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
	InitializeShard(shardId uint32, controller LeaderController) error
	CloseShard(shardId uint32) error
}

type sessionManager struct {
	sync.Mutex
	controllerSupplier LeaderControllerSupplier
	sessions           map[SessionId]*session
	sessionCounters    map[uint32]SessionId
	// channels to close heartbeat listeners
	closeChannels []chan error
}

func NewSessionManager() SessionManager {
	return &sessionManager{
		Mutex:           sync.Mutex{},
		sessions:        make(map[SessionId]*session),
		sessionCounters: make(map[uint32]SessionId),
		closeChannels:   make([]chan error, 0),
	}
}

func (sm *sessionManager) UseLeaderControllerSupplier(controllerSupplier LeaderControllerSupplier) SessionManager {
	sm.controllerSupplier = controllerSupplier
	return sm
}

func SingleLeaderController(controller LeaderController) LeaderControllerSupplier {
	return func(_ uint32) (LeaderController, error) {
		return controller, nil
	}
}

func (sm *sessionManager) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	timeout := time.Duration(request.SessionTimeoutMs) * time.Millisecond
	if timeout > MaxTimeout {
		return nil, errors.Wrap(ErrorInvalidSessionTimeout, fmt.Sprintf("timeoutMs=%d", request.SessionTimeoutMs))
	}
	sm.Lock()
	defer sm.Unlock()
	sm.sessionCounters[request.ShardId]++

	sessionId := sm.sessionCounters[request.ShardId]

	controller, err := sm.controllerSupplier(request.ShardId)
	if err != nil {
		return nil, errors.Wrap(err, "could not register session")
	}
	resp, err := controller.Write(&proto.WriteRequest{
		ShardId: &request.ShardId,
		Puts: []*proto.PutRequest{{
			Key:     Key(sessionId),
			Payload: []byte{},
		}},
	})
	if err != nil || resp.Puts[0].Status != proto.Status_OK {
		if err == nil {
			err = errors.New("failed to register session")
		}
		return nil, err
	}

	s := sm.startSession(sessionId, request.ShardId, timeout)
	sm.sessions[sessionId] = s

	return &proto.CreateSessionResponse{SessionId: uint32(s.id)}, nil

}

func (sm *sessionManager) KeepAlive(server proto.OxiaClient_KeepAliveServer) error {
	sm.Lock()
	defer sm.Unlock()
	closeChannel := make(chan error, 1)
	sm.closeChannels = append(sm.closeChannels, closeChannel)

	go sm.receiveHeartbeats(server, closeChannel)
	return <-closeChannel

}

func (sm *sessionManager) CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	sm.Lock()
	defer sm.Unlock()
	sessionId := SessionId(request.SessionId)
	s, found := sm.sessions[sessionId]
	if !found {
		return nil, errors.Wrap(ErrorSessionNotFound, fmt.Sprintf("sessionId=%d", request.SessionId))
	}
	delete(sm.sessions, sessionId)
	err := s.close(true)
	if err != nil {
		return nil, err
	}
	return &proto.CloseSessionResponse{}, nil
}

func (sm *sessionManager) InitializeShard(shardId uint32, controller LeaderController) error {
	sm.Lock()
	defer sm.Unlock()
	if !false {
		return nil
	}
	if _, loaded := sm.sessionCounters[shardId]; loaded {
		return errors.New("session already loaded")
	}
	sessionIds, err := sm.readSessions(shardId, controller)
	if err != nil {
		return err
	}
	maxSessionId := SessionId(0)
	for _, sessionId := range sessionIds {
		if sessionId > maxSessionId {
			maxSessionId = sessionId
		}
		sm.startSession(sessionId, shardId, 0)
	}
	sm.sessionCounters[shardId] = maxSessionId + 1
	return nil
}

func (sm *sessionManager) readSessions(shardId uint32, controller LeaderController) ([]SessionId, error) {
	// TODO resolve deadlock caused by  controller's Mutex not being reentrant and that this is called from BecomeLeader
	resp, err := controller.Read(&proto.ReadRequest{
		ShardId: &shardId,
		Gets:    nil,
		Lists: []*proto.ListRequest{
			{
				StartInclusive: KeyPrefix,
				EndExclusive:   KeyPrefix + "/",
			},
		},
	})
	if err != nil {
		return nil, err
	}
	sessionIds := make([]SessionId, 0, len(resp.Lists[0].Keys))
	err = nil
	for _, key := range resp.Lists[0].Keys {
		sessionId, err2 := KeyToId(key)
		if err2 != nil {
			err = multierr.Append(err, err2)
		} else {
			sessionIds = append(sessionIds, sessionId)
		}
	}
	if err != nil {
		return nil, err
	}
	return sessionIds, nil
}

func (sm *sessionManager) CloseShard(shardId uint32) error {
	sm.Lock()
	defer sm.Unlock()
	var err error = nil
	for _, s := range sm.sessions {
		if s.shardId == shardId {
			delete(sm.sessions, s.id)
			err = multierr.Append(err,
				s.close(false))
		}
	}
	delete(sm.sessionCounters, shardId)
	return err
}

func (sm *sessionManager) Close() error {
	sm.Lock()
	defer sm.Unlock()
	var err error = nil
	for _, s := range sm.sessions {
		err = multierr.Append(err, s.close(false))
	}
	for _, ch := range sm.closeChannels {
		ch <- nil
		close(ch)
	}
	return err
}

// --- Session

type session struct {
	sync.Mutex
	id          SessionId
	shardId     uint32
	timeout     time.Duration
	sm          *sessionManager
	attached    bool
	heartbeatCh chan *proto.Heartbeat
	closeCh     chan error
}

func (s *session) close(delete bool) error {
	s.Lock()
	defer s.Unlock()
	// TODO send close msg to client if err == nil?
	if s.closeCh != nil {
		close(s.closeCh)
		s.closeCh = nil
	}
	if s.heartbeatCh != nil {
		close(s.heartbeatCh)
		s.heartbeatCh = nil
	}

	if !delete {
		return nil
	}
	// Delete ephemeral data associated with this session
	controller, err := s.sm.controllerSupplier(s.shardId)
	if err != nil {
		return err
	}
	sessionKey := Key(s.id)
	// Read "index"
	list, err := controller.Read(&proto.ReadRequest{
		ShardId: &s.shardId,
		Lists: []*proto.ListRequest{{
			StartInclusive: sessionKey,
			EndExclusive:   sessionKey + "/",
		}},
	})
	if err != nil {
		return err
	}
	// Delete ephemerals
	var deletes []*proto.DeleteRequest
	for _, key := range list.Lists[0].Keys {
		unescapedKey, err := url.PathUnescape(key[:len(sessionKey)])
		if err != nil {
			// TODO maybe only log the error and continue. Although this error should never happen
			return err
		}
		if unescapedKey != "" {
			deletes = append(deletes, &proto.DeleteRequest{
				Key: unescapedKey,
			})
		}
	}
	_, err = controller.Write(&proto.WriteRequest{
		ShardId: &s.shardId,
		Puts:    nil,
		Deletes: deletes,
		// Delete the index and the session keys
		DeleteRanges: []*proto.DeleteRangeRequest{
			{
				StartInclusive: sessionKey,
				EndExclusive:   sessionKey + "/",
			},
		},
	})
	return err

}

func (s *session) heartbeat(heartbeat *proto.Heartbeat) {
	s.Lock()
	s.Unlock()
	if s.heartbeatCh != nil {
		s.heartbeatCh <- heartbeat
	}
}

func (sm *sessionManager) startSession(sessionId SessionId, shardId uint32, timeout time.Duration) *session {
	s := &session{
		Mutex:       sync.Mutex{},
		id:          sessionId,
		shardId:     shardId,
		timeout:     timeout,
		sm:          sm,
		heartbeatCh: make(chan *proto.Heartbeat, 1),
	}
	go s.waitForHeartbeats()
	return s

}

func (s *session) waitForHeartbeats() {
	s.Lock()
	timeout := s.timeout
	s.Unlock()
	for {
		select {
		case heartbeat := <-s.heartbeatCh:
			if heartbeat == nil {
				// The channel is closed, so the session must be closing
				return
			}
		case <-time.After(timeout):
			err := s.close(false)
			err = errors.Wrap(err, "session timed out")
			// TODO Log
		}
	}
}

func (sm *sessionManager) receiveHeartbeats(stream proto.OxiaClient_KeepAliveServer, closeChannel chan error) {

	firstBeat, err := stream.Recv()
	if err != nil {
		sm.Lock()
		sm.sendErrorAndRemove(closeChannel, err)
		sm.Unlock()
		return
	}
	if firstBeat == nil {
		// Got nil because we are shutting down
		return
	}

	sessionId := SessionId(firstBeat.SessionId)
	sm.Lock()
	s, found := sm.sessions[sessionId]
	if !found {
		sm.sendErrorAndRemove(closeChannel, errors.Wrap(ErrorSessionNotFound, fmt.Sprintf("sessioId=%d", sessionId)))
		sm.Unlock()
		return
	}
	s.Lock()
	if s.attached {
		s.Unlock()
		sm.sendErrorAndRemove(closeChannel, errors.New(fmt.Sprintf("session (sessionId=%d) already attached", sessionId)))
		sm.Unlock()
		return
	} else {
		s.attached = true
		s.closeCh = closeChannel
	}

	s.Unlock()
	sm.Unlock()

	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			sm.Lock()
			sm.sendErrorAndRemove(closeChannel, errors.New(fmt.Sprintf("session (sessionId=%d) already attached", sessionId)))
			sm.Unlock()
		}
		if heartbeat == nil {
			// closing already
			return
		}
		s.heartbeat(heartbeat)
	}

}

func (sm *sessionManager) sendErrorAndRemove(closeChannel chan error, err error) {
	closeChannel <- err
	close(closeChannel)
	for i, c := range sm.closeChannels {
		if c == closeChannel {
			sm.closeChannels = append(sm.closeChannels[:i], sm.closeChannels[i+1:]...)
		}
	}
}
