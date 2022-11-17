package internal

import (
	"context"
	"errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/proto"
	"sync"
)

var (
	ErrorUnknownShardRange = errors.New("unknown shard range")
)

type ShardManager interface {
	io.Closer
	Start()
	Get(key string) uint32
	GetAll() []uint32
	Leader(shardId uint32) string
}

type shardManagerImpl struct {
	sync.Mutex
	shardStrategy ShardStrategy
	clientPool    common.ClientPool
	serviceUrl    string
	shards        map[uint32]Shard
	closeC        chan bool
	logger        zerolog.Logger
}

func NewShardManager(shardStrategy ShardStrategy, clientPool common.ClientPool, serviceUrl string) ShardManager {
	return &shardManagerImpl{
		shardStrategy: shardStrategy,
		clientPool:    clientPool,
		serviceUrl:    serviceUrl,
		shards:        make(map[uint32]Shard),
		closeC:        make(chan bool),
		logger:        log.With().Str("component", "shardManager").Logger(),
	}
}

func (s *shardManagerImpl) Close() error {
	close(s.closeC)
	return nil
}

func (s *shardManagerImpl) Start() {
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())

	go s.receiveWithRecovery(ctx, &wg)
	go func() {
		if _, ok := <-s.closeC; !ok {
			cancel()
		}
	}()

	wg.Wait()
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
	s.Lock()
	defer s.Unlock()

	shardIds := make([]uint32, 0, len(s.shards))
	for shardId := range s.shards {
		shardIds = append(shardIds, shardId)
	}
	return shardIds
}

func (s *shardManagerImpl) Leader(shardId uint32) string {
	s.Lock()
	defer s.Unlock()

	if shard, ok := s.shards[shardId]; ok {
		return shard.Leader
	}
	panic("shard not found")
}

func (s *shardManagerImpl) isClosed() bool {
	select {
	case _, ok := <-s.closeC:
		if !ok {
			return true
		}
	default:
		//noop
	}
	return false
}

func (s *shardManagerImpl) receiveWithRecovery(ctx context.Context, wg *sync.WaitGroup) {
	for {
		err := s.receive(ctx, wg)
		if err != nil && s.isClosed() {
			return
		}
	}
}

func (s *shardManagerImpl) receive(ctx context.Context, wg *sync.WaitGroup) error {
	rpc, err := s.clientPool.GetClientRpc(s.serviceUrl)
	if err != nil {
		return err
	}

	request := proto.ShardAssignmentsRequest{}

	stream, err := rpc.ShardAssignments(ctx, &request)
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
			wg.Done()
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
}

func overlap(a HashRange, b HashRange) bool {
	return !(a.MinInclusive >= b.MaxExclusive || a.MaxExclusive <= b.MinInclusive)
}
