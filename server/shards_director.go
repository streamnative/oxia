package server

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/proto"
	"sync"
)

type ShardsDirector interface {
	io.Closer

	GetShardsAssignments(callback func(*proto.ShardsAssignments))

	GetManager(shardId uint32, create bool) (ShardManager, error)
}

type shardsDirector struct {
	mutex *sync.Mutex
	cond  *sync.Cond

	assignments   *proto.ShardsAssignments
	shardManagers map[uint32]ShardManager
	identityAddr  string

	log zerolog.Logger
}

func NewShardsDirector(identityAddr string) ShardsDirector {
	mutex := &sync.Mutex{}
	return &shardsDirector{
		mutex: mutex,
		cond:  sync.NewCond(mutex),

		identityAddr:  identityAddr,
		shardManagers: make(map[uint32]ShardManager),
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

func (s *shardsDirector) GetManager(shardId uint32, create bool) (ShardManager, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	manager, ok := s.shardManagers[shardId]
	if !ok && create {
		s.shardManagers[shardId] = NewShardManager(shardId, s.identityAddr /* TODO */, nil)
	} else {
		s.log.Debug().Uint32("shard", shardId).Msg("This node is not hosting shard")
		return nil, errors.Errorf("This node is not leader for shard %d", shardId)
	}
	return manager, nil
}

func (s *shardsDirector) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for shard, manager := range s.shardManagers {
		if err := manager.Close(); err != nil {
			s.log.Error().
				Err(err).
				Uint32("shard", shard).
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
