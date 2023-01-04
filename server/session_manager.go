package server

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	pb "google.golang.org/protobuf/proto"
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

var ErrorSessionNotFound = errors.New("session not found")

var ErrorInvalidSessionTimeout = errors.New("invalid session timeout")

type SessionId uint64

func hexId(sessionId SessionId) string {
	return fmt.Sprintf("%016x", sessionId)
}

func SessionKey(sessionId SessionId) string {
	return fmt.Sprintf("%s%s/", KeyPrefix, hexId(sessionId))
}

func KeyToId(key string) (SessionId, error) {
	if len(key) != len(KeyPrefix)+17 || KeyPrefix != key[:len(KeyPrefix)] || "/" != key[len(key)-1:] {
		return 0, errors.New("invalid sessionId key " + key)
	}
	s := key[len(KeyPrefix):]
	s = s[:len(s)-1]
	longInt, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, err
	}
	return SessionId(longInt), nil
}

// PutDecorator performs two operations on Put requests that have a session ID.
// First, it checks whether the session is alive. If it is not, it errors out.
// Then it adds an "index" entry as the child of the session key.
var PutDecorator kv.PutCustomizer = &putDecorator{}

var versionNotExists int64 = -1

type putDecorator struct{}

func (_ *putDecorator) AdditionalData(putReq *proto.PutRequest) *proto.PutRequest {
	sessionId := putReq.SessionId
	if sessionId == nil {
		return nil
	}
	return &proto.PutRequest{
		Key:             SessionKey(SessionId(*sessionId)) + url.PathEscape(putReq.Key),
		Payload:         []byte{},
		ExpectedVersion: &versionNotExists,
	}
}

func (_ *putDecorator) CheckApplicability(batch kv.WriteBatch, putReq *proto.PutRequest) (proto.Status, error) {
	sessionId := putReq.SessionId
	if sessionId == nil {
		return proto.Status_OK, nil
	}
	var _, closer, err = batch.Get(SessionKey(SessionId(*sessionId)))
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
	CreateSession(*proto.CreateSessionRequest) (*proto.CreateSessionResponse, error)
	KeepAlive(proto.OxiaClient_KeepAliveServer) error
	CloseSession(*proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
	InitializeShard(shardId uint32, controller LeaderController) error
	CloseShard(shardId uint32)
}

var _ SessionManager = (*sessionManager)(nil)

type sessionManager struct {
	sync.Mutex
	controllerSupplier LeaderControllerSupplier
	sessions           map[SessionId]*session
	sessionCounters    map[uint32]SessionId
	// channels to close heartbeat listeners
	closeChannels []chan error
	log           zerolog.Logger
}

func NewSessionManager(controllerSupplier LeaderControllerSupplier) SessionManager {
	return &sessionManager{
		Mutex:              sync.Mutex{},
		sessions:           make(map[SessionId]*session),
		sessionCounters:    make(map[uint32]SessionId),
		closeChannels:      make([]chan error, 0),
		controllerSupplier: controllerSupplier,
		log: log.With().
			Str("component", "session-manager").
			Logger(),
	}
}

func (sm *sessionManager) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	timeout := time.Duration(request.SessionTimeoutMs) * time.Millisecond
	if timeout > MaxTimeout {
		return nil, errors.Wrap(ErrorInvalidSessionTimeout, fmt.Sprintf("timeoutMs=%d", request.SessionTimeoutMs))
	}
	sm.Lock()
	defer sm.Unlock()
	if _, initialized := sm.sessionCounters[request.ShardId]; !initialized {
		return nil, errors.Errorf("session management for shard %d not initialized", request.ShardId)
	}
	sm.sessionCounters[request.ShardId]++

	sessionId := (SessionId(request.ShardId) << 32) + sm.sessionCounters[request.ShardId]

	metadata := &proto.SessionMetadata{TimeoutMS: uint64(timeout.Milliseconds())}

	marshalledMetadata, err := pb.Marshal(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal session metadata")
	}
	controller, err := sm.controllerSupplier(request.ShardId)
	if err != nil {
		return nil, errors.Wrap(err, "could not register session")
	}
	resp, err := controller.Write(&proto.WriteRequest{
		ShardId: &request.ShardId,
		Puts: []*proto.PutRequest{{
			Key:     SessionKey(sessionId),
			Payload: marshalledMetadata,
		}},
	})
	if err != nil || resp.Puts[0].Status != proto.Status_OK {
		if err == nil {
			err = errors.New("failed to register session")
		}
		return nil, err
	}

	s := sm.startSession(sessionId, request.ShardId, metadata)
	sm.sessions[sessionId] = s

	return &proto.CreateSessionResponse{SessionId: uint64(s.id)}, nil

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
	s.Lock()
	defer s.Unlock()
	s.closeChannels()
	err := s.delete()
	if err != nil {
		return nil, err
	}
	return &proto.CloseSessionResponse{}, nil
}

func (sm *sessionManager) InitializeShard(shardId uint32, controller LeaderController) error {
	sm.Lock()
	defer sm.Unlock()
	if _, loaded := sm.sessionCounters[shardId]; loaded {
		return errors.New("shard already loaded")
	}
	sessions, err := sm.readSessions(shardId, controller)
	if err != nil {
		return err
	}
	maxSessionId := SessionId(0)
	for sessionId, sessionMetadata := range sessions {
		if sessionId > maxSessionId {
			maxSessionId = sessionId
		}
		sm.startSession(sessionId, shardId, sessionMetadata)
	}
	sm.sessionCounters[shardId] = maxSessionId + 1
	return nil
}

func (sm *sessionManager) readSessions(shardId uint32, controller LeaderController) (map[SessionId]*proto.SessionMetadata, error) {
	listResp, err := controller.readWithoutLocking(&proto.ReadRequest{
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
	var gets []*proto.GetRequest
	for _, key := range listResp.Lists[0].Keys {
		gets = append(gets, &proto.GetRequest{
			Key:            key,
			IncludePayload: true,
		})
	}
	getResp, err := controller.readWithoutLocking(&proto.ReadRequest{
		ShardId: &shardId,
		Gets:    gets,
		Lists:   nil,
	})
	if err != nil {
		return nil, err
	}

	result := map[SessionId]*proto.SessionMetadata{}

	for i, metaEntry := range getResp.Gets {
		key := gets[i].Key
		if metaEntry.Status != proto.Status_OK {
			sm.log.Warn().
				Uint32("shard", shardId).
				Msgf("error reading session metadata at `%s`, status: %d", key, metaEntry.Status)
			continue
		}
		payload := metaEntry.Payload
		metadata := proto.SessionMetadata{}
		err = pb.Unmarshal(payload, &metadata)
		if err != nil {
			sm.log.Warn().
				Uint32("shard", shardId).
				Err(err).
				Msgf("error unmarshalling session metadata at `%s`", key)
			continue
		}
		sessionId, err := KeyToId(key)
		if err != nil {
			sm.log.Warn().
				Uint32("shard", shardId).
				Err(err).
				Msgf("error parsing session key `%s`", key)
			continue
		}
		result[sessionId] = &metadata
	}

	return result, nil
}

func (sm *sessionManager) CloseShard(shardId uint32) {
	sm.Lock()
	defer sm.Unlock()
	for _, s := range sm.sessions {
		if s.shardId == shardId {
			delete(sm.sessions, s.id)
			s.Lock()
			s.closeChannels()
			s.Unlock()
		}
	}
	delete(sm.sessionCounters, shardId)
}

func (sm *sessionManager) Close() error {
	sm.Lock()
	defer sm.Unlock()
	for _, s := range sm.sessions {
		s.Lock()
		s.closeChannels()
		s.Unlock()
	}
	for _, ch := range sm.closeChannels {
		ch <- nil
		close(ch)
	}
	return nil
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
	log         zerolog.Logger
}

func (s *session) closeChannels() {
	if s.closeCh != nil {
		close(s.closeCh)
		s.closeCh = nil
	}
	if s.heartbeatCh != nil {
		close(s.heartbeatCh)
		s.heartbeatCh = nil
	}
}

func (s *session) delete() error {
	// Delete ephemeral data associated with this session
	controller, err := s.sm.controllerSupplier(s.shardId)
	if err != nil {
		return err
	}
	sessionKey := SessionKey(s.id)
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
	defer s.Unlock()
	if s.heartbeatCh != nil {
		s.heartbeatCh <- heartbeat
	}
}

func (sm *sessionManager) startSession(sessionId SessionId, shardId uint32, sessionMetadata *proto.SessionMetadata) *session {
	s := &session{
		Mutex:       sync.Mutex{},
		id:          sessionId,
		shardId:     shardId,
		timeout:     time.Duration(sessionMetadata.TimeoutMS) * time.Millisecond,
		sm:          sm,
		heartbeatCh: make(chan *proto.Heartbeat, 1),
		log: sm.log.With().
			Uint32("shard", shardId).
			Str("session-id", hexId(sessionId)).Logger(),
	}
	go s.waitForHeartbeats()
	return s

}

func (s *session) waitForHeartbeats() {
	s.log.Debug().Msg("Waiting for heartbeats")
	timeout := s.timeout
	for {
		select {
		case heartbeat := <-s.heartbeatCh:
			if heartbeat == nil {
				// The channel is closed, so the session must be closing
				return
			}
		case <-time.After(timeout):
			s.log.Info().
				Msg("Session timed out")

			s.Lock()
			s.closeChannels()
			err := s.delete()

			if err != nil {
				s.log.Error().Err(err).
					Msg("Failed to delete session")
			}
			s.Unlock()
		}
	}
}

func (sm *sessionManager) receiveHeartbeats(stream proto.OxiaClient_KeepAliveServer, closeChannel chan error) {

	firstBeat, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		// Got EOF because we are shutting down
		return
	} else if err != nil {
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
		if errors.Is(err, io.EOF) {
			// closing already
			return
		} else if err != nil {
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
