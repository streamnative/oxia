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
	"io"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
)

// --- Session

type session struct {
	io.Closer
	sync.Mutex
	id             SessionId
	clientIdentity string
	shardId        int64
	timeout        time.Duration
	sm             *sessionManager
	heartbeatCh    chan bool
	cancel         context.CancelFunc
	ctx            context.Context
	log            *slog.Logger
}

func startSession(sessionId SessionId, sessionMetadata *proto.SessionMetadata, sm *sessionManager) *session {
	s := &session{
		id:             sessionId,
		clientIdentity: sessionMetadata.Identity,
		timeout:        time.Duration(sessionMetadata.TimeoutMs) * time.Millisecond,
		sm:             sm,
		heartbeatCh:    make(chan bool, 1),

		log: slog.With(
			slog.String("client-identity", sessionMetadata.Identity),
			slog.String("component", "session"),
			slog.Int64("session-id", int64(sessionId)),
			slog.String("namespace", sm.namespace),
			slog.Int64("shard", sm.shardId),
		),
	}
	sm.sessions.Put(sessionId, s)

	s.ctx, s.cancel = context.WithCancel(context.Background())

	go common.DoWithLabels(s.ctx, map[string]string{
		"oxia":            "session",
		"client-identity": sessionMetadata.Identity,
		"session-id":      fmt.Sprintf("%d", sessionId),
		"namespace":       sm.namespace,
		"shard":           fmt.Sprintf("%d", sm.shardId),
	}, s.waitForHeartbeats)

	s.log.Info("Session started",
		slog.Duration("session-timeout", s.timeout))
	return s
}

func (s *session) Close() {
	s.Lock()
	defer s.Unlock()
	s.cancel()
	if s.heartbeatCh != nil {
		close(s.heartbeatCh)
		s.heartbeatCh = nil
	}
	s.log.Debug("Session channels closed")
}

func (s *session) delete() error {
	// Delete ephemeral data associated with this session
	sessionKey := SessionKey(s.id)
	// Read "index"
	keys, err := s.sm.leaderController.ListBlock(context.Background(), &proto.ListRequest{
		Shard:          &s.shardId,
		StartInclusive: sessionKey + "/",
		EndExclusive:   sessionKey + "//",
	})
	if err != nil {
		return err
	}
	// Delete ephemerals
	var deletes []*proto.DeleteRequest
	s.log.Debug(
		"Keys to delete",
		slog.Any("keys", keys),
	)
	for _, key := range keys {
		unescapedKey, err := url.PathUnescape(key[len(sessionKey)+1:])
		if err != nil {
			s.log.Error(
				"Invalid session key",
				slog.Any("error", err),
				slog.String("key", sessionKey),
			)
			continue
		}
		if unescapedKey != "" {
			deletes = append(deletes, &proto.DeleteRequest{
				Key: unescapedKey,
			})
		}
	}

	// Delete the base session metadata
	deletes = append(deletes, &proto.DeleteRequest{
		Key: sessionKey,
	})
	_, err = s.sm.leaderController.Write(context.Background(), &proto.WriteRequest{
		Shard:   &s.shardId,
		Puts:    nil,
		Deletes: deletes,
		// Delete the whole index of ephemeral keys for the session
		DeleteRanges: []*proto.DeleteRangeRequest{
			{
				StartInclusive: sessionKey + "/",
				EndExclusive:   sessionKey + "//",
			},
		},
	})
	s.log.Info("Session cleanup complete",
		slog.Int("keys-deleted", len(deletes)))
	return err
}

func (s *session) heartbeat() {
	s.Lock()
	defer s.Unlock()
	if s.heartbeatCh != nil {
		s.heartbeatCh <- true
	}
}

func (s *session) waitForHeartbeats() {
	s.Lock()
	heartbeatChannel := s.heartbeatCh
	s.Unlock()
	s.log.Debug("Waiting for heartbeats")
	for {
		var timer = time.NewTimer(s.timeout)
		var timeoutCh = timer.C
		select {
		case heartbeat := <-heartbeatChannel:
			if !heartbeat {
				// The channel is closed, so the session must be closing
				return
			}
			timer.Reset(s.timeout)
		case <-timeoutCh:
			s.log.Warn("Session expired")

			s.Close()
			err := s.delete()

			if err != nil {
				s.log.Error(
					"Failed to delete session",
					slog.Any("error", err),
				)
			}

			s.sm.Lock()
			s.sm.sessions.Remove(s.id)
			s.sm.expiredSessions.Inc()
			s.sm.Unlock()
		}
	}
}
