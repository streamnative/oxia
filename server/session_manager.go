// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	pb "google.golang.org/protobuf/proto"
	"io"
	"net/url"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/proto"
	"oxia/server/kv"
	"strconv"
	"sync"
	"time"
)

const (
	KeyPrefix = common.InternalKeyPrefix + "session/"
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
	KeepAlive(sessionId int64) error
	CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
	Initialize() error
}

var _ SessionManager = (*sessionManager)(nil)

type sessionManager struct {
	sync.RWMutex
	leaderController *leaderController
	shardId          int64
	sessions         map[SessionId]*session
	log              zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	createdSessions metrics.Counter
	closedSessions  metrics.Counter
	expiredSessions metrics.Counter
	activeSessions  metrics.Gauge
}

func NewSessionManager(ctx context.Context, namespace string, shardId int64, controller *leaderController) SessionManager {
	labels := metrics.LabelsForShard(namespace, shardId)
	sm := &sessionManager{
		sessions:         make(map[SessionId]*session),
		shardId:          shardId,
		leaderController: controller,
		log: log.With().
			Str("component", "session-manager").
			Str("namespace", namespace).
			Int64("shard", shardId).
			Int64("term", controller.term).
			Logger(),

		createdSessions: metrics.NewCounter("oxia_server_sessions_created",
			"The total number of sessions created", "count", labels),
		closedSessions: metrics.NewCounter("oxia_server_sessions_closed",
			"The total number of sessions closed", "count", labels),
		expiredSessions: metrics.NewCounter("oxia_server_sessions_expired",
			"The total number of sessions expired", "count", labels),
	}

	sm.ctx, sm.cancel = context.WithCancel(ctx)

	sm.activeSessions = metrics.NewGauge("oxia_server_session_active",
		"The number of sessions currently active", "count", labels, func() int64 {
			sm.RLock()
			defer sm.RUnlock()
			return int64(len(sm.sessions))
		})

	return sm
}

func (sm *sessionManager) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return sm.createSession(request, common.MinSessionTimeout)
}

func (sm *sessionManager) createSession(request *proto.CreateSessionRequest, minTimeout time.Duration) (*proto.CreateSessionResponse, error) {

	timeout := time.Duration(request.SessionTimeoutMs) * time.Millisecond
	if timeout > common.MaxSessionTimeout || timeout < minTimeout {
		return nil, errors.Wrap(common.ErrorInvalidSessionTimeout, fmt.Sprintf("timeoutMs=%d", request.SessionTimeoutMs))
	}
	sm.Lock()
	defer sm.Unlock()

	metadata := &proto.SessionMetadata{TimeoutMs: uint32(timeout.Milliseconds())}

	marshalledMetadata, err := pb.Marshal(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal session metadata")
	}
	id, resp, err := sm.leaderController.write(sm.ctx, func(id int64) *proto.WriteRequest {
		return &proto.WriteRequest{
			ShardId: &request.ShardId,
			Puts: []*proto.PutRequest{{
				Key:   SessionKey(SessionId(id)),
				Value: marshalledMetadata,
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

	sm.createdSessions.Inc()
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

func (sm *sessionManager) KeepAlive(sessionId int64) error {
	sm.RLock()
	s, err := sm.getSession(sessionId)
	sm.RUnlock()
	if err != nil {
		return err
	}
	s.heartbeat()
	return nil
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

	sm.closedSessions.Inc()
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
	list, err := sm.leaderController.ListSliceNoMutex(context.Background(), &proto.ListRequest{
		ShardId:        &sm.shardId,
		StartInclusive: KeyPrefix,
		EndExclusive:   KeyPrefix + "/",
	})
	if err != nil {
		return nil, err
	}

	result := map[SessionId]*proto.SessionMetadata{}

	for _, key := range list {
		metaEntry, err := sm.leaderController.db.Get(&proto.GetRequest{
			Key:          key,
			IncludeValue: true,
		})
		if err != nil {
			return nil, err
		}

		if metaEntry.Status != proto.Status_OK {
			sm.log.Warn().
				Str("key", key).
				Stringer("status", metaEntry.Status).
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
		value := metaEntry.Value
		metadata := proto.SessionMetadata{}
		err = pb.Unmarshal(value, &metadata)
		if err != nil {
			sm.log.Warn().
				Err(err).
				Int64("session-id", int64(sessionId)).
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
	sm.cancel()
	for _, s := range sm.sessions {
		delete(sm.sessions, s.id)
		s.Lock()
		s.closeChannels()
		s.Unlock()
	}

	sm.activeSessions.Unregister()
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
	if err != nil && !errors.Is(err, kv.ErrorKeyNotFound) {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	return proto.Status_OK, nil
}

func (_ *updateCallback) OnDelete(batch kv.WriteBatch, key string) error {
	se, err := kv.GetStorageEntry(batch, key)
	if errors.Is(err, kv.ErrorKeyNotFound) {
		return nil
	}
	if err == nil && se.SessionId != nil {
		_, err = deleteShadow(batch, key, se)
	}
	return err
}
