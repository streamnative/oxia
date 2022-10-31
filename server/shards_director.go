package server

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/proto"
	"sync"
)

type ShardsDirector interface {
	io.Closer

	GetShardsAssignments(callback func(*proto.ShardsAssignments))

	GetManager(shardId ShardId, create bool) (ShardManager, error)
}

type ShardId string

type shardsDirector struct {
	mutex *sync.Mutex
	cond  *sync.Cond

	assignments   *proto.ShardsAssignments
	shardManagers map[ShardId]ShardManager
	identityAddr  string

	log zerolog.Logger
}

func NewShardsDirector(identityAddr string) ShardsDirector {
	mutex := &sync.Mutex{}
	return &shardsDirector{
		mutex: mutex,
		cond:  sync.NewCond(mutex),

		identityAddr:  identityAddr,
		shardManagers: make(map[ShardId]ShardManager),
		log: log.With().
			Str("component", "shards-director").
			Logger(),
	}
}

func (s *shardsDirector) GetShardsAssignments(callback func(*proto.ShardsAssignments)) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.assignments != nil {
		callback(s.assignments)
	}

	oldAssignments := s.assignments
	for {
		s.cond.Wait()

		if oldAssignments != s.assignments {
			callback(s.assignments)
			oldAssignments = s.assignments
		}
	}
}

func (s *shardsDirector) GetManager(shardId ShardId, create bool) (ShardManager, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	manager, ok := s.shardManagers[shardId]
	if ok {
		return manager, nil
	} else if create {
		w := NewInMemoryWal(shardId)
		kv := NewInMemoryKVStore()
		pool := common.NewClientPool()
		sm, err := NewShardManager(shardId, s.identityAddr, pool, w, kv)
		if err != nil {
			return nil, err
		}
		s.shardManagers[shardId] = sm
		return sm, nil
	} else {
		s.log.Debug().Str("shard", string(shardId)).
			Msg("This node is not hosting shard")
		return nil, errors.Errorf("This node is not leader for shard %d", shardId)
	}

}

func (s *shardsDirector) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for shard, manager := range s.shardManagers {
		if err := manager.Close(); err != nil {
			s.log.Error().
				Err(err).
				Str("shard", string(shard)).
				Msg("Failed to shutdown leader controller")
		}
	}
	return nil
}

func (s *shardsDirector) shouldLead(shard *proto.ShardStatus) bool {
	return shard.Leader.InternalUrl == s.identityAddr
}

func (s *shardsDirector) shouldFollow(shard *proto.ShardStatus) bool {
	for _, f := range shard.Followers {
		if f.InternalUrl == s.identityAddr {
			return true
		}
	}

	return false
}
