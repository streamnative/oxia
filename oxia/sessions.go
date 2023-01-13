package oxia

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/metadata"
	"oxia/common"
	"oxia/oxia/internal"
	"oxia/proto"
	"sync"
	"time"
)

func NewSessions(ctx context.Context, shardManager internal.ShardManager, pool common.ClientPool, options clientOptions) *sessions {
	s := &sessions{
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
	ctx             context.Context
	shardManager    internal.ShardManager
	pool            common.ClientPool
	sessionsByShard map[uint32]*clientSession
	log             zerolog.Logger
	clientOpts      clientOptions
}

func (s *sessions) executeWithSessionId(ctx context.Context, shardId uint32, callback func(int64, error)) {
	s.Lock()
	session, found := s.sessionsByShard[shardId]
	if !found {
		session = s.startSession(ctx, shardId)
		s.sessionsByShard[shardId] = session
	}
	session.executeWithId(ctx, callback)
	defer s.Unlock()

}

func (s *sessions) startSession(ctx context.Context, shardId uint32) *clientSession {
	cs := &clientSession{
		shardId:  shardId,
		sessions: s,
		log: log.With().
			Str("component", "session").
			Uint32("shard-id", shardId).Logger(),
	}
	cs.start(ctx)
	return cs
}

type clientSession struct {
	sync.Mutex
	started   chan error
	shardId   uint32
	sessionId int64
	log       zerolog.Logger
	sessions  *sessions
}

func (cs *clientSession) executeWithId(ctx context.Context, callback func(int64, error)) {
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
	case <-ctx.Done():
		if ctx.Err() != nil && ctx.Err() != context.Canceled {
			callback(-1, ctx.Err())

		}
	}
}

func (cs *clientSession) start(ctx context.Context) {
	cs.started = make(chan error)
	backOff := common.NewBackOff(ctx)
	go common.DoWithLabels(map[string]string{
		"oxia":  "session-start",
		"shard": fmt.Sprintf("%d", cs.shardId),
	}, func() { cs.createSessionWithRetries(ctx, backOff) })

}

func (cs *clientSession) createSessionWithRetries(ctx context.Context, backOff backoff.BackOff) {
	err := backoff.RetryNotify(func() error { return cs.createSession(ctx) },
		backOff, func(err error, duration time.Duration) {
			if err != context.Canceled {
				cs.log.Error().Err(err).
					Dur("retry-after", duration).
					Msg("Error while starting session")
			}

		})
	if err != nil && err != context.Canceled {
		cs.Lock()
		cs.started <- err
		close(cs.started)
		cs.Unlock()
	}
}

func (cs *clientSession) createSession(ctx context.Context) error {
	leader := cs.sessions.shardManager.Leader(cs.shardId)
	rpc, err := cs.sessions.pool.GetClientRpc(leader)
	if err != nil {
		return err
	}
	createSessionResponse, err := rpc.CreateSession(ctx, &proto.CreateSessionRequest{
		ShardId:          cs.shardId,
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

	go common.DoWithLabels(map[string]string{
		"oxia":  "session-keep-alive",
		"shard": fmt.Sprintf("%d", cs.shardId),
	}, func() { cs.keepAlive(rpc) })

	return nil
}

func (cs *clientSession) keepAlive(rpc proto.OxiaClientClient) {
	cs.sessions.Lock()
	cs.Lock()
	timeout := cs.sessions.clientOpts.sessionTimeout
	ctx := cs.sessions.ctx
	shardId := cs.shardId
	sessionId := cs.sessionId
	cs.Unlock()
	cs.sessions.Unlock()

	timer := time.NewTicker(2 * timeout / 3)

	ctx = metadata.AppendToOutgoingContext(ctx, common.MetadataShardId, fmt.Sprintf("%d", shardId))
	ctx = metadata.AppendToOutgoingContext(ctx, common.MetadataSessionId, fmt.Sprintf("%d", sessionId))
	client, err := rpc.KeepAlive(ctx)
	if err != nil {
		return
		// TODO log, retry
	}
	for {
		select {
		case <-timer.C:
			err = client.Send(&proto.SessionHeartbeat{
				ShardId:   shardId,
				SessionId: sessionId,
			})
			if err != nil {
				// TODO log, retry
				return
			}
		case <-ctx.Done():
			// TODO rpc.Close()?
			return
		}
	}

}
