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
	MaxTimeout = 5 * time.Minute
)

type SessionId int64

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

// --- SessionManager

type SessionManager interface {
	io.Closer
	CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error)
	KeepAlive(sessionId int64, stream proto.OxiaClient_KeepAliveServer) error
	CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
	Initialize() error
}

var _ SessionManager = (*sessionManager)(nil)

type sessionManager struct {
	sync.Mutex
	leaderController *leaderController
	shardId          uint32
	sessions         map[SessionId]*session
	log              zerolog.Logger
}

func NewSessionManager(shardId uint32, controller *leaderController) SessionManager {
	return &sessionManager{
		sessions:         make(map[SessionId]*session),
		shardId:          shardId,
		leaderController: controller,
		log: log.With().
			Str("component", "session-manager").
			Uint32("shard", shardId).
			Logger(),
	}
}

func (sm *sessionManager) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	timeout := time.Duration(request.SessionTimeoutMs) * time.Millisecond
	if timeout > MaxTimeout {
		return nil, errors.Wrap(common.ErrorInvalidSessionTimeout, fmt.Sprintf("timeoutMs=%d", request.SessionTimeoutMs))
	}
	sm.Lock()
	defer sm.Unlock()

	metadata := &proto.SessionMetadata{TimeoutMs: uint32(timeout.Milliseconds())}

	marshalledMetadata, err := pb.Marshal(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal session metadata")
	}
	id, resp, err := sm.leaderController.write(func(id int64) *proto.WriteRequest {
		return &proto.WriteRequest{
			ShardId: &request.ShardId,
			Puts: []*proto.PutRequest{{
				Key:     SessionKey(SessionId(id)),
				Payload: marshalledMetadata,
			}},
		}
	})
	sessionId := SessionId(id)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register session")
	}

	if resp.Puts[0].Status != proto.Status_OK {
		return nil, errors.Errorf("failed to register session. invalid status %#v", resp.Puts[0].Status)
	}

	s := startSession(sessionId, metadata, sm)
	sm.sessions[sessionId] = s

	return &proto.CreateSessionResponse{SessionId: int64(s.id)}, nil

}

func (sm *sessionManager) getSession(sessionId int64) (*session, error) {
	s, found := sm.sessions[SessionId(sessionId)]
	if !found {
		sm.log.Warn().
			Int64("session-id", sessionId).
			Msg("Session not found")
		return nil, common.ErrorInvalidSession
	}
	return s, nil
}

func (sm *sessionManager) KeepAlive(sessionId int64, server proto.OxiaClient_KeepAliveServer) error {
	sm.Lock()
	s, err := sm.getSession(sessionId)
	sm.Unlock()
	if err != nil {
		return err
	}
	closeCh, err := s.attach(server)
	if err != nil {
		return err
	}

	return <-closeCh

}

func (sm *sessionManager) CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	sm.Lock()
	s, err := sm.getSession(request.SessionId)
	if err != nil {
		sm.Unlock()
		return nil, err
	}
	delete(sm.sessions, s.id)
	sm.Unlock()
	s.Lock()
	defer s.Unlock()
	s.closeChannels()
	err = s.delete()
	if err != nil {
		return nil, err
	}
	return &proto.CloseSessionResponse{}, nil
}

func (sm *sessionManager) Initialize() error {
	sm.Lock()
	defer sm.Unlock()
	sessions, err := sm.readSessions()
	if err != nil {
		return err
	}
	for sessionId, sessionMetadata := range sessions {
		startSession(sessionId, sessionMetadata, sm)
	}
	return nil
}

func (sm *sessionManager) readSessions() (map[SessionId]*proto.SessionMetadata, error) {
	listResp, err := sm.leaderController.db.ProcessRead(&proto.ReadRequest{
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
	var gets []*proto.GetRequest
	for _, key := range listResp.Lists[0].Keys {
		gets = append(gets, &proto.GetRequest{
			Key:            key,
			IncludePayload: true,
		})
	}
	getResp, err := sm.leaderController.db.ProcessRead(&proto.ReadRequest{
		ShardId: &sm.shardId,
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
				Str("key", key).
				Str("status", fmt.Sprintf("%#v", metaEntry.Status)).
				Msgf("error reading session metadata")
			continue
		}
		sessionId, err := KeyToId(key)
		if err != nil {
			sm.log.Warn().
				Err(err).
				Str("key", key).
				Msgf("error parsing session key")
			continue
		}
		payload := metaEntry.Payload
		metadata := proto.SessionMetadata{}
		err = pb.Unmarshal(payload, &metadata)
		if err != nil {
			sm.log.Warn().
				Err(err).
				Int32("session-id", int32(sessionId)).
				Str("key", key).
				Msgf("error unmarshalling session metadata")
			continue
		}

		result[sessionId] = &metadata
	}

	return result, nil
}

func (sm *sessionManager) Close() error {
	sm.Lock()
	defer sm.Unlock()
	for _, s := range sm.sessions {
		delete(sm.sessions, s.id)
		s.Lock()
		s.closeChannels()
		s.Unlock()
	}
	return nil
}

type updateCallback struct{}

var SessionUpdateOperationCallback kv.UpdateOperationCallback = &updateCallback{}

func (_ *updateCallback) OnPut(batch kv.WriteBatch, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	if existingEntry != nil && existingEntry.SessionId != nil {
		// We are overwriting an ephemeral value, let's delete its shadow
		if status, err := deleteShadow(batch, request.Key, existingEntry); err != nil {
			return status, err
		}
	}

	sessionId := request.SessionId
	if sessionId != nil {

		// We are adding an ephemeral value, let's check if the session exists
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
		// Create the session shadow entry
		err = batch.Put(SessionKey(SessionId(*sessionId))+url.PathEscape(request.Key), []byte{})
		if err != nil {
			return proto.Status_SESSION_DOES_NOT_EXIST, err
		}
	}
	return proto.Status_OK, nil
}

func deleteShadow(batch kv.WriteBatch, key string, existingEntry *proto.StorageEntry) (proto.Status, error) {
	existingSessionId := SessionId(*existingEntry.SessionId)
	err := batch.Delete(SessionKey(existingSessionId) + url.PathEscape(key))
	if err != nil {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	return proto.Status_OK, nil
}

func (_ *updateCallback) OnDelete(batch kv.WriteBatch, key string) error {
	se, err := kv.GetStorageEntry(batch, key)
	if err == nil && se.SessionId != nil {
		_, err = deleteShadow(batch, key, se)
	}
	return err
}
