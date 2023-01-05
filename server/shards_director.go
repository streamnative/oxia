package server

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"google.golang.org/grpc/status"
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

	config    Config
	leaders   map[uint32]LeaderController
	followers map[uint32]FollowerController

	kvFactory              kv.KVFactory
	walFactory             wal.WalFactory
	replicationRpcProvider ReplicationRpcProvider
	closed                 bool
	log                    zerolog.Logger
}

func NewShardsDirector(config Config, walFactory wal.WalFactory, kvFactory kv.KVFactory, provider ReplicationRpcProvider) ShardsDirector {
	return &shardsDirector{
		config:                 config,
		walFactory:             walFactory,
		kvFactory:              kvFactory,
		leaders:                make(map[uint32]LeaderController),
		followers:              make(map[uint32]FollowerController),
		replicationRpcProvider: provider,
		log: log.With().
			Str("component", "shards-director").
			Logger(),
	}
}

func (s *shardsDirector) GetLeader(shardId uint32) (LeaderController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, common.ErrorAlreadyClosed
	}

	if leader, ok := s.leaders[shardId]; ok {
		// There is already a leader controller for this shard
		return leader, nil
	}

	s.log.Debug().
		Uint32("shard", shardId).
		Msg("This node is not hosting shard")
	return nil, status.Errorf(common.CodeNodeIsNotLeader, "node is not leader for shard %d", shardId)
}

func (s *shardsDirector) GetFollower(shardId uint32) (FollowerController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, common.ErrorAlreadyClosed
	}

	if follower, ok := s.followers[shardId]; ok {
		// There is already a follower controller for this shard
		return follower, nil
	}

	s.log.Debug().
		Uint32("shard", shardId).
		Msg("This node is not hosting shard")
	return nil, status.Errorf(common.CodeNodeIsNotFollower, "node is not follower for shard %d", shardId)
}

func (s *shardsDirector) GetOrCreateLeader(shardId uint32) (LeaderController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, common.ErrorAlreadyClosed
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
	if lc, err := NewLeaderController(s.config, shardId, s.replicationRpcProvider, s.walFactory, s.kvFactory); err != nil {
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
		return nil, common.ErrorAlreadyClosed
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
	if fc, err := NewFollowerController(s.config, shardId, s.walFactory, s.kvFactory); err != nil {
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
	var err error

	for _, leader := range s.leaders {
		err = multierr.Append(err, leader.Close())
	}

	for _, follower := range s.followers {
		err = multierr.Append(err, follower.Close())
	}

	return err
}
