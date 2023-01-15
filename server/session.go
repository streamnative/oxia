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
	"github.com/rs/zerolog"
	"net/url"
	"oxia/common"
	"oxia/proto"
	"oxia/server/util"
	"sync"
	"time"
)

// --- Session

type session struct {
	sync.Mutex
	id          SessionId
	shardId     uint32
	timeout     time.Duration
	sm          *sessionManager
	attached    bool
	heartbeatCh chan *proto.SessionHeartbeat
	cancel      context.CancelFunc
	ctx         context.Context
	log         zerolog.Logger
}

func startSession(sessionId SessionId, sessionMetadata *proto.SessionMetadata, sm *sessionManager) *session {
	s := &session{
		id:          sessionId,
		timeout:     time.Duration(sessionMetadata.TimeoutMs) * time.Millisecond,
		sm:          sm,
		heartbeatCh: make(chan *proto.SessionHeartbeat, 1),

		log: sm.log.With().
			Str("component", "session").
			Int64("session-id", int64(sessionId)).Logger(),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	go s.waitForHeartbeats()
	s.log.Debug().Msg("Session started")
	return s
}

func (s *session) closeChannels() {
	s.cancel()
	if s.heartbeatCh != nil {
		close(s.heartbeatCh)
		s.heartbeatCh = nil
	}
	s.log.Debug().Msg("Session channels closed")
	s.sm.Lock()
	delete(s.sm.sessions, s.id)
	s.sm.Unlock()
}

func (s *session) delete() error {
	// Delete ephemeral data associated with this session
	sessionKey := SessionKey(s.id)
	// Read "index"
	list, err := s.sm.leaderController.Read(&proto.ReadRequest{
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
	s.log.Debug().Strs("keys", list.Lists[0].Keys).Msg("Keys to delete")
	for _, key := range list.Lists[0].Keys {
		unescapedKey, err := url.PathUnescape(key[len(sessionKey):])
		if err != nil {
			s.log.Error().Err(err).Str("key", sessionKey).Msg("Invalid session key")
			continue
		}
		if unescapedKey != "" {
			deletes = append(deletes, &proto.DeleteRequest{
				Key: unescapedKey,
			})
		}
	}
	_, err = s.sm.leaderController.Write(&proto.WriteRequest{
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
	s.log.Debug().Msg("Session deleted")
	return err

}

func (s *session) attach(server proto.OxiaClient_KeepAliveServer) error {
	s.Lock()
	if s.attached {
		s.log.Error().Msg("Session already attached")
		s.Unlock()
		return common.ErrorInvalidSession
	}
	s.attached = true
	s.Unlock()
	return s.receiveHeartbeats(server)
}

func (s *session) heartbeat(heartbeat *proto.SessionHeartbeat) error {
	s.Lock()
	defer s.Unlock()
	if s.heartbeatCh != nil {
		s.heartbeatCh <- heartbeat
	}
	return nil
}

func (s *session) waitForHeartbeats() {
	s.Lock()
	heartbeatChannel := s.heartbeatCh
	s.Unlock()
	s.log.Debug().Msg("Waiting for heartbeats")
	for {
		var timer = time.NewTimer(s.timeout)
		var timeoutCh = timer.C
		select {

		case heartbeat := <-heartbeatChannel:
			if heartbeat == nil {
				// The channel is closed, so the session must be closing
				return
			}
			timer.Reset(s.timeout)
		case <-timeoutCh:
			s.log.Info().
				Msg("Session expired")

			s.Lock()
			s.closeChannels()
			s.sm.expiredSessions.Inc()
			err := s.delete()

			if err != nil {
				s.log.Error().Err(err).
					Msg("Failed to delete session")
			}
			s.Unlock()
		}
	}
}

func (s *session) receiveHeartbeats(stream proto.OxiaClient_KeepAliveServer) error {
	s.Lock()
	reader := util.ReadStream[proto.SessionHeartbeat](
		stream,
		s.heartbeat,
		map[string]string{
			"oxia":    "receive-session-heartbeats",
			"session": fmt.Sprintf("%d", s.id),
			"shard":   fmt.Sprintf("%d", s.shardId),
		}, s.ctx, s.log)
	s.Unlock()
	return reader.Run()
}
