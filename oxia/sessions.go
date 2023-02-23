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

package oxia

import (
	"context"
	"errors"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/status"
	"oxia/common"
	"oxia/oxia/internal"
	"oxia/proto"
	"sync"
	"time"
)

func newSessions(ctx context.Context, shardManager internal.ShardManager, pool common.ClientPool, options clientOptions) *sessions {
	s := &sessions{
		clientIdentity:  options.identity,
		ctx:             ctx,
		shardManager:    shardManager,
		pool:            pool,
		sessionsByShard: map[uint32]*clientSession{},
		clientOpts:      options,
		log:             log.With().Str("component", "oxia-session-manager").Logger(),
	}
	return s
}

type sessions struct {
	sync.Mutex
	clientIdentity  string
	ctx             context.Context
	shardManager    internal.ShardManager
	pool            common.ClientPool
	sessionsByShard map[uint32]*clientSession
	log             zerolog.Logger
	clientOpts      clientOptions
}

func (s *sessions) executeWithSessionId(shardId uint32, callback func(int64, error)) {
	s.Lock()
	defer s.Unlock()
	session, found := s.sessionsByShard[shardId]
	if !found {
		session = s.startSession(shardId)
		s.sessionsByShard[shardId] = session
	}
	session.executeWithId(callback)
}

func (s *sessions) startSession(shardId uint32) *clientSession {
	cs := &clientSession{
		shardId:  shardId,
		sessions: s,
		ctx:      s.ctx,
		started:  make(chan error),
		log: log.With().
			Str("component", "session").
			Uint32("shard", shardId).Logger(),
	}
	cs.log.Debug().Msg("Creating session")
	go common.DoWithLabels(map[string]string{
		"oxia":  "session-start",
		"shard": fmt.Sprintf("%d", cs.shardId),
	}, func() { cs.createSessionWithRetries() })
	return cs
}

type clientSession struct {
	sync.Mutex
	started   chan error
	shardId   uint32
	sessionId int64
	log       zerolog.Logger
	sessions  *sessions
	ctx       context.Context
}

func (cs *clientSession) executeWithId(callback func(int64, error)) {
	select {
	case err := <-cs.started:
		if err != nil {
			callback(-1, err)
			cs.sessions.Lock()
			defer cs.sessions.Unlock()
			cs.Lock()
			defer cs.Unlock()
			delete(cs.sessions.sessionsByShard, cs.shardId)
		} else {
			cs.Lock()
			callback(cs.sessionId, nil)
			cs.Unlock()
		}
	case <-cs.ctx.Done():
		if cs.ctx.Err() != nil && cs.ctx.Err() != context.Canceled {
			callback(-1, cs.ctx.Err())
		}
	}
}

func (cs *clientSession) createSessionWithRetries() {
	backOff := common.NewBackOff(cs.ctx)
	err := backoff.RetryNotify(cs.createSession,
		backOff, func(err error, duration time.Duration) {
			if err != context.Canceled {
				cs.log.Error().Err(err).
					Dur("retry-after", duration).
					Msg("Error while creating session")
			}

		})
	if err != nil && err != context.Canceled {
		cs.Lock()
		cs.started <- err
		close(cs.started)
		cs.Unlock()
	}
}

func (cs *clientSession) createSession() error {
	rpc, err := cs.getRpc()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(cs.ctx, cs.sessions.clientOpts.requestTimeout)
	defer cancel()
	createSessionResponse, err := rpc.CreateSession(ctx, &proto.CreateSessionRequest{
		ShardId:          cs.shardId,
		ClientIdentity:   cs.sessions.clientIdentity,
		SessionTimeoutMs: uint32(cs.sessions.clientOpts.sessionTimeout.Milliseconds()),
	})
	if err != nil {
		return err
	}
	sessionId := createSessionResponse.SessionId
	cs.Lock()
	defer cs.Unlock()
	cs.sessionId = sessionId
	cs.log = cs.log.With().Int64("session-id", sessionId).Logger()
	close(cs.started)
	cs.log.Debug().Msg("Successfully created session")

	go common.DoWithLabels(map[string]string{
		"oxia":    "session-keep-alive",
		"shard":   fmt.Sprintf("%d", cs.shardId),
		"session": fmt.Sprintf("%x016", cs.sessionId),
	}, func() {

		backOff := common.NewBackOff(cs.sessions.ctx)
		err := backoff.RetryNotify(func() error {
			err := cs.keepAlive()
			if status.Code(err) == common.CodeInvalidSession {
				cs.log.Error().Err(err).Msg("Session is no longer valid")

				cs.sessions.Lock()
				defer cs.sessions.Unlock()
				cs.Lock()
				defer cs.Unlock()
				delete(cs.sessions.sessionsByShard, cs.shardId)
				return backoff.Permanent(err)
			}
			return err
		}, backOff, func(err error, duration time.Duration) {
			log.Logger.Debug().Err(err).
				Dur("retry-after", duration).
				Msg("Failed to send session heartbeat, retrying later")
		})
		if !errors.Is(err, context.Canceled) {
			cs.log.Error().Err(err).Msg("Failed to keep alive session.")
		}
	})

	return nil
}

func (cs *clientSession) getRpc() (proto.OxiaClientClient, error) {
	leader := cs.sessions.shardManager.Leader(cs.shardId)
	return cs.sessions.pool.GetClientRpc(leader)
}

func (cs *clientSession) keepAlive() error {
	cs.sessions.Lock()
	cs.Lock()
	timeout := cs.sessions.clientOpts.sessionTimeout
	ctx := cs.sessions.ctx
	shardId := cs.shardId
	sessionId := cs.sessionId
	cs.Unlock()
	cs.sessions.Unlock()

	tickTime := timeout / 10
	if tickTime < 2*time.Second {
		tickTime = 2 * time.Second
	}

	ticker := time.NewTicker(tickTime)
	defer ticker.Stop()

	rpc, err := cs.getRpc()
	if err != nil {
		return err
	}

	for {
		select {
		case <-ticker.C:
			_, err = rpc.KeepAlive(ctx, &proto.SessionHeartbeat{ShardId: shardId, SessionId: sessionId})
			if err != nil {
				return err
			}
		case <-ctx.Done():
			ctx, cancel := context.WithTimeout(context.Background(), cs.sessions.clientOpts.requestTimeout)
			rpc, err = cs.getRpc()
			if err != nil {
				cancel()
				return err
			}
			_, err = rpc.CloseSession(ctx, &proto.CloseSessionRequest{
				ShardId:   shardId,
				SessionId: sessionId,
			})

			cancel()
			return err
		}
	}

}
