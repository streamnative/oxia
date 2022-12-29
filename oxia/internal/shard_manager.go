package internal

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/proto"
	"sync"
	"time"
)

type ShardManager interface {
	io.Closer
	Get(key string) uint32
	GetAll() []uint32
	Leader(shardId uint32) string
}

type shardManagerImpl struct {
	sync.RWMutex
	updatedCondition common.ConditionContext

	shardStrategy  ShardStrategy
	clientPool     common.ClientPool
	serviceAddress string
	shards         map[uint32]Shard
	ctx            context.Context
	cancel         context.CancelFunc
	logger         zerolog.Logger
	requestTimeout time.Duration
}

func NewShardManager(shardStrategy ShardStrategy, clientPool common.ClientPool, serviceAddress string, requestTimeout time.Duration) (ShardManager, error) {
	sm := &shardManagerImpl{
		shardStrategy:  shardStrategy,
		clientPool:     clientPool,
		serviceAddress: serviceAddress,
		shards:         make(map[uint32]Shard),
		requestTimeout: requestTimeout,
		logger:         log.With().Str("component", "shardManager").Logger(),
	}

	sm.updatedCondition = common.NewConditionContext(sm)
	sm.ctx, sm.cancel = context.WithCancel(context.Background())

	if err := sm.start(); err != nil {
		return nil, errors.Wrap(err, "oxia: failed retrieve the initial list of shard assignments")
	}

	return sm, nil
}

func (s *shardManagerImpl) Close() error {
	s.cancel()
	return nil
}

func (s *shardManagerImpl) start() error {
	s.Lock()
	defer s.Unlock()

	go common.DoWithLabels(map[string]string{
		"oxia": "receive-shard-updates",
	}, s.receiveWithRecovery)

	ctx, cancel := context.WithTimeout(s.ctx, s.requestTimeout)
	defer cancel()

	return s.updatedCondition.Wait(ctx)
}

func (s *shardManagerImpl) Get(key string) uint32 {
	s.Lock()
	defer s.Unlock()

	predicate := s.shardStrategy.Get(key)

	for _, shard := range s.shards {
		if predicate(shard) {
			return shard.Id
		}
	}
	panic("shard not found")
}

func (s *shardManagerImpl) GetAll() []uint32 {
	s.RLock()
	defer s.RUnlock()

	shardIds := make([]uint32, 0, len(s.shards))
	for shardId := range s.shards {
		shardIds = append(shardIds, shardId)
	}
	return shardIds
}

func (s *shardManagerImpl) Leader(shardId uint32) string {
	s.RLock()
	defer s.RUnlock()

	if shard, ok := s.shards[shardId]; ok {
		return shard.Leader
	}
	panic("shard not found")
}

func (s *shardManagerImpl) isClosed() bool {
	return s.ctx.Err() != nil
}

func (s *shardManagerImpl) receiveWithRecovery() {
	backOff := common.NewBackOff(s.ctx)
	err := backoff.RetryNotify(
		func() error {
			err := s.receive(backOff)
			if s.isClosed() {
				s.logger.Debug().Err(err).Msg("Closed")
				return nil
			}
			return err
		},
		backOff,
		func(err error, duration time.Duration) {
			s.logger.Warn().Err(err).
				Dur("retry-after", duration).
				Msg("Failed receiving shard assignments, retrying later")
		},
	)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed receiving shard assignments")
	}
}

func (s *shardManagerImpl) receive(backOff backoff.BackOff) error {
	rpc, err := s.clientPool.GetClientRpc(s.serviceAddress)
	if err != nil {
		return err
	}

	request := proto.ShardAssignmentsRequest{}

	stream, err := rpc.ShardAssignments(s.ctx, &request)
	if err != nil {
		return err
	}

	for {
		if response, err := stream.Recv(); err != nil {
			return err
		} else {
			shards := make([]Shard, len(response.Assignments))
			for i, assignment := range response.Assignments {
				shards[i] = toShard(assignment)
			}
			s.update(shards)
			backOff.Reset()
		}
	}
}

func (s *shardManagerImpl) update(updates []Shard) {
	s.Lock()
	defer s.Unlock()

	for _, update := range updates {
		if _, ok := s.shards[update.Id]; !ok {
			//delete overlaps
			for shardId, existing := range s.shards {
				if overlap(update.HashRange, existing.HashRange) {
					s.logger.Info().Msgf("Deleting shard %+v as it overlaps with %+v", existing, update)
					delete(s.shards, shardId)
				}
			}
		}
		s.shards[update.Id] = update
	}

	s.updatedCondition.Signal()
}

func overlap(a HashRange, b HashRange) bool {
	return !(a.MinInclusive > b.MaxInclusive || a.MaxInclusive < b.MinInclusive)
}
