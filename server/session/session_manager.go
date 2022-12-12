package session

import (
	"github.com/pkg/errors"
	"io"
	"net/url"
	"oxia/common"
	"oxia/proto"
	"oxia/server/kv"
	"sync"
	"time"
)

const (
	KeyPrefix = common.InternalKeyPrefix + "session/"
	Timeout   = 10 * time.Second
)

func Key(sessionId string) string {
	return KeyPrefix + sessionId + "/"
}

var PutDecorator = &putDecorator{}
var VersionNotExists int64 = -1

type putDecorator struct{}

func (_ *putDecorator) AdditionalData(putReq *proto.PutRequest) *proto.PutRequest {
	sessionId := putReq.SessionId
	if sessionId == nil || *sessionId == "" {
		return nil
	}
	return &proto.PutRequest{
		Key:             Key(*sessionId) + url.PathEscape(putReq.Key),
		Payload:         []byte{},
		ExpectedVersion: &VersionNotExists,
	}
}

func (_ *putDecorator) CheckApplicability(batch kv.WriteBatch, putReq *proto.PutRequest) (proto.Status, error) {
	sessionId := putReq.SessionId
	if sessionId == nil || *sessionId == "" {
		return proto.Status_OK, nil
	}
	_, closer, err := batch.Get(Key(*sessionId))
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

type Controller interface {
	Write(write *proto.WriteRequest) (*proto.WriteResponse, error)
	Read(read *proto.ReadRequest) (*proto.ReadResponse, error)
	SessionManager() SessionManager
}

type director interface {
	GetLeaderController(shardId uint32) (Controller, error)
}

// --- SessionManagerFactory

type SessionManagerFactory interface {
	io.Closer
	NewSessionManager(shardId uint32, controller Controller) (SessionManager, error)
	HandleSessionStream(ss director, stream proto.OxiaClient_SessionServer) error
}

type sessionManagerFactory struct {
	sync.Mutex
	unnamedSessions       map[int]*session
	unnamedSessionCounter int
}

func NewSessionManagerFactory() SessionManagerFactory {
	return &sessionManagerFactory{
		Mutex:           sync.Mutex{},
		unnamedSessions: make(map[int]*session),
	}
}

func (smf *sessionManagerFactory) NewSessionManager(shardId uint32, controller Controller) (SessionManager, error) {
	sm := &sessionManager{
		Mutex:      sync.Mutex{},
		shardId:    shardId,
		controller: controller,
	}
	return sm, nil
}

func (smf *sessionManagerFactory) HandleSessionStream(sd director, stream proto.OxiaClient_SessionServer) error {
	s := startSession(nil, "")
	smf.Lock()
	sessionNumber := smf.unnamedSessionCounter
	smf.unnamedSessionCounter++
	smf.unnamedSessions[sessionNumber] = s
	smf.Unlock()
	go receiveHeartbeats(sessionNumber, smf, s, sd, stream)
	return <-s.closeCh
}

func (smf *sessionManagerFactory) Close() error {
	smf.Lock()
	defer smf.Unlock()
	for _, session := range smf.unnamedSessions {
		session.close(nil)
	}
	return nil
}

// --- SessionManager

type SessionManager interface {
	io.Closer
	Initialize() error
}

type sessionManager struct {
	sync.Mutex
	controller Controller
	sessions   map[string]*session
	shardId    uint32
}

func (sm *sessionManager) Close() error {
	sm.Lock()
	defer sm.Unlock()
	for _, s := range sm.sessions {
		s.close(nil)
	}
	return nil
}

func (sm *sessionManager) Initialize() error {
	sessions, err := sm.readSessions()
	if err != nil {
		return err
	}
	for _, s := range sessions {
		startSession(sm, s)
	}
	return nil
}

func (sm *sessionManager) readSessions() ([]string, error) {
	// TODO resolve deadlock caused by  controller's Mutex not being reentrant and that this is called from BecomeLeader
	resp, err := sm.controller.Read(&proto.ReadRequest{
		ShardId: &sm.shardId,
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
	return resp.Lists[0].Keys, nil
}

func (sm *sessionManager) clearSession(id string) error {
	sessionKey := Key(id)
	list, err := sm.controller.Read(&proto.ReadRequest{
		ShardId: &sm.shardId,
		Lists: []*proto.ListRequest{{
			StartInclusive: sessionKey,
			EndExclusive:   sessionKey + "/",
		}},
	})
	if err != nil {
		return err
	}
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
	_, err = sm.controller.Write(&proto.WriteRequest{
		ShardId: &sm.shardId,
		Puts:    nil,
		Deletes: deletes,
		DeleteRanges: []*proto.DeleteRangeRequest{
			{
				StartInclusive: sessionKey,
				EndExclusive:   sessionKey + "/",
			},
		},
	})
	return err
}

// --- Session

type session struct {
	sync.Mutex
	id          string
	sm          *sessionManager
	recovering  bool
	heartbeatCh chan *proto.Heartbeat
	closeCh     chan error
}

func (s *session) close(err error) {

	s.sm.Lock()
	delete(s.sm.sessions, s.id)
	s.sm.Unlock()
	s.Lock()
	defer s.Unlock()
	// TODO send close msg to client if err == nil?
	if s.closeCh != nil {
		s.closeCh <- err
		close(s.closeCh)
		s.closeCh = nil
	}
	if s.heartbeatCh != nil {
		close(s.heartbeatCh)
		s.heartbeatCh = nil
	}

	go func() {
		err := s.sm.clearSession(s.id)
		if err != nil {
			// TODO
		}
	}()
}

func (s *session) heartbeat(heartbeat *proto.Heartbeat) {
	s.Lock()
	s.Unlock()
	if s.heartbeatCh != nil {
		s.heartbeatCh <- heartbeat
	}
}

func startSession(s *sessionManager, sessionId string) *session {
	se := &session{
		Mutex:       sync.Mutex{},
		id:          sessionId,
		recovering:  sessionId != "",
		sm:          s,
		closeCh:     make(chan error, 1),
		heartbeatCh: make(chan *proto.Heartbeat, 1),
	}
	go waitForHeartbeats(se)

	return se

}

func waitForHeartbeats(se *session) {
	for {
		select {
		case heartbeat := <-se.heartbeatCh:
			if heartbeat == nil {
				// The channel is closed, so the session must be closing
				return
			}
		case <-time.After(Timeout):
			se.close(errors.New("session timed out"))
		}
	}
}

func receiveHeartbeats(sessionNumber int, smf *sessionManagerFactory, session *session, sd director, stream proto.OxiaClient_SessionServer) {
	firstBeat, err := stream.Recv()
	if err != nil {
		session.close(err)
		return
	}
	if firstBeat == nil {
		session.close(nil)
		return
	}
	shardId := firstBeat.ShardId
	sessionId := firstBeat.SessionId
	if sessionId == "" {
		session.close(errors.New("oxia: invalid sessionId"))
		return
	}
	session.Lock()
	session.id = sessionId
	session.Unlock()
	lc, err := sd.GetLeaderController(shardId)
	if err != nil {
		session.close(err)
		return
	}
	smf.Lock()
	sm := lc.SessionManager().(*sessionManager)
	sm.Lock()
	if existingSession, found := sm.sessions[sessionId]; found {
		if existingSession.recovering {
			session.close(nil)
			session = existingSession
			session.sm = sm
			session.heartbeat(firstBeat)
		} else {
			delete(smf.unnamedSessions, sessionNumber)
			sm.Unlock()
			smf.Unlock()
			session.close(errors.New("oxia: session already exists"))
			return
		}
	}
	sm.sessions[sessionId] = session
	delete(smf.unnamedSessions, sessionNumber)
	sm.Unlock()
	smf.Unlock()
	resp, err := lc.Write(&proto.WriteRequest{
		ShardId: &shardId,
		Puts: []*proto.PutRequest{{
			Key:     Key(sessionId),
			Payload: []byte{},
		}},
	})
	if err != nil || resp.Puts[0].Status != proto.Status_OK {
		if err == nil {
			err = errors.New("failed to register session")
		}
		session.close(err)
		return
	}

	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			session.close(err)
			return
		}
		if heartbeat == nil {
			// closing already
			return
		}
		session.heartbeat(heartbeat)
	}

}
