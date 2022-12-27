package server

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
)

type ShardsDirector interface {
	io.Closer

	GetLeader(shardId uint32) (LeaderController, error)
	GetFollower(shardId uint32) (FollowerController, error)

	GetOrCreateLeader(shardId uint32) (LeaderController, error)
	GetOrCreateFollower(shardId uint32) (FollowerController, error)
}

type shardsDirector struct {
	sync.Mutex

	leaders   map[uint32]LeaderController
	followers map[uint32]FollowerController

	kvFactory  kv.KVFactory
	walFactory wal.WalFactory
	pool       common.ClientPool
	closed     bool
	log        zerolog.Logger
}

func NewShardsDirector(walFactory wal.WalFactory, kvFactory kv.KVFactory) ShardsDirector {
	return &shardsDirector{
		walFactory: walFactory,
		kvFactory:  kvFactory,
		leaders:    make(map[uint32]LeaderController),
		followers:  make(map[uint32]FollowerController),
		pool:       common.NewClientPool(),
		log: log.With().
			Str("component", "shards-director").
			Logger(),
	}
}

func (s *shardsDirector) GetLeader(shardId uint32) (LeaderController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, ErrorAlreadyClosed
	}

	if leader, ok := s.leaders[shardId]; ok {
		// There is already a leader controller for this shard
		return leader, nil
	}

	s.log.Debug().
		Uint32("shard", shardId).
		Msg("This node is not hosting shard")
	return nil, errors.Wrapf(ErrorNodeIsNotLeader, "node is not leader for shard %d", shardId)
}

func (s *shardsDirector) GetFollower(shardId uint32) (FollowerController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, ErrorAlreadyClosed
	}

	if follower, ok := s.followers[shardId]; ok {
		// There is already a follower controller for this shard
		return follower, nil
	}

	s.log.Debug().
		Uint32("shard", shardId).
		Msg("This node is not hosting shard")
	return nil, errors.Wrapf(ErrorNodeIsNotFollower, "node is not follower for shard %d", shardId)
}

func (s *shardsDirector) GetOrCreateLeader(shardId uint32) (LeaderController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, ErrorAlreadyClosed
	}

	if leader, ok := s.leaders[shardId]; ok {
		// There is already a leader controller for this shard
		return leader, nil
	} else if follower, ok := s.followers[shardId]; ok {
		// There is an existing follower controller
		// Let's close it and before creating the leader controller

		if err := follower.Close(); err != nil {
			return nil, err
		}

		delete(s.followers, shardId)
	}

	// Create new leader controller
	if lc, err := NewLeaderController(shardId, NewReplicationRpcProvider(s.pool), s.walFactory, s.kvFactory); err != nil {
		return nil, err
	} else {
		s.leaders[shardId] = lc
		return lc, nil
	}
}

func (s *shardsDirector) GetOrCreateFollower(shardId uint32) (FollowerController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, ErrorAlreadyClosed
	}

	if follower, ok := s.followers[shardId]; ok {
		// There is already a follower controller for this shard
		return follower, nil
	} else if leader, ok := s.leaders[shardId]; ok {
		// There is an existing leader controller
		// Let's close it and before creating the follower controller

		if err := leader.Close(); err != nil {
			return nil, err
		}

		delete(s.leaders, shardId)
	}

	// Create new follower controller
	if fc, err := NewFollowerController(shardId, s.walFactory, s.kvFactory); err != nil {
		return nil, err
	} else {
		s.followers[shardId] = fc
		return fc, nil
	}
}

func (s *shardsDirector) Close() error {
	s.Lock()
	defer s.Unlock()

	s.closed = true

	for shard, leader := range s.leaders {
		if err := leader.Close(); err != nil {
			s.log.Error().
				Err(err).
				Uint32("shard", shard).
				Msg("Failed to shutdown leader controller")
		}
	}

	for shard, follower := range s.followers {
		if err := follower.Close(); err != nil {
			s.log.Error().
				Err(err).
				Uint32("shard", shard).
				Msg("Failed to shutdown leader controller")
		}
	}
	return s.pool.Close()
}
