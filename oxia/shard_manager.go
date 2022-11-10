package oxia

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"oxia/common"
	"oxia/proto"
	"sync"
)

type shardManager interface {
	start() <-chan bool
	get(key string) (uint32, error)
	getAll() []uint32
	leader(shardId uint32) (string, error)
}

var (
	ErrorShardNotFound     = errors.New("Shard not found")
	ErrorUnknownShardRange = errors.New("Unknown toShard range")
)

type shardManagerImpl struct {
	sync.Mutex
	shardStrategy ShardStrategy
	clientPool    common.ClientPool
	serviceUrl    string
	ready         chan bool
	shards        map[uint32]Shard
	logger        zerolog.Logger
}

func newShardManager(shardStrategy ShardStrategy, clientPool common.ClientPool, serviceUrl string) shardManager {
	return &shardManagerImpl{
		shardStrategy: shardStrategy,
		clientPool:    clientPool,
		serviceUrl:    serviceUrl,
		ready:         make(chan bool, 1),
		shards:        make(map[uint32]Shard),
		logger:        log.With().Str("component", "shardManager").Logger(),
	}
}

func (s *shardManagerImpl) start() <-chan bool {
	go s.receiveWithRecovery()
	return s.ready
}

func (s *shardManagerImpl) get(key string) (uint32, error) {
	s.Lock()
	defer s.Unlock()

	predicate := s.shardStrategy.Get(key)

	for _, shard := range s.shards {
		if predicate(shard) {
			return shard.Id, nil
		}
	}
	return 0, ErrorShardNotFound
}

func (s *shardManagerImpl) getAll() []uint32 {
	s.Lock()
	defer s.Unlock()

	shardIds := make([]uint32, 0)
	for shardId := range s.shards {
		shardIds = append(shardIds, shardId)
	}
	return shardIds
}

func (s *shardManagerImpl) leader(shardId uint32) (string, error) {
	s.Lock()
	defer s.Unlock()

	if shard, ok := s.shards[shardId]; ok {
		return shard.Leader, nil
	} else {
		return "", ErrorShardNotFound
	}
}

func (s *shardManagerImpl) receiveWithRecovery() {
	for {
		if err := <-s.receive(); err != nil {
			//TODO fail on non-retryable errors
		}
	}
}

func (s *shardManagerImpl) receive() <-chan error {
	errs := make(chan error, 1)

	rpc, err := s.clientPool.GetClientRpc(s.serviceUrl)
	if err != nil {
		errs <- err
		return errs
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request := proto.ShardAssignmentsRequest{}

	stream, err := rpc.ShardAssignments(ctx, &request)
	if err != nil {
		errs <- err
		return errs
	}

	for {
		if response, err := stream.Recv(); err != nil {
			errs <- err
			return errs
		} else {
			updates := make([]Shard, 0)
			for _, assignment := range response.Assignments {
				if shard, err := toShard(assignment); err != nil {
					//unknown shard boundaries, ignore
				} else {
					updates = append(updates, shard)
				}
			}
			s.update(updates)
		}
	}
}

func (s *shardManagerImpl) update(updates []Shard) {
	s.Lock()
	defer s.Unlock()

	for _, update := range updates {
		if _, ok := s.shards[update.Id]; !ok {
			//delete overlaps
			for id, existing := range s.shards {
				// TODO move to shard strategy
				// TODO protobuf support shard deletion in shard assigments
				if overlap(&update.HashRange, &existing.HashRange) {
					s.logger.Info().Msgf("Deleting shard %+v as it overlaps with %+v", existing, update)
					delete(s.shards, id)
				}
			}
		}
		s.shards[update.Id] = update
	}
	s.ready <- true
}
