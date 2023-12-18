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
	"io"
	"log/slog"
	"sync"

	"go.uber.org/multierr"
	"google.golang.org/grpc/status"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
)

type ShardsDirector interface {
	io.Closer

	GetLeader(shardId int64) (LeaderController, error)
	GetFollower(shardId int64) (FollowerController, error)

	GetOrCreateLeader(namespace string, shardId int64) (LeaderController, error)
	GetOrCreateFollower(namespace string, shardId int64) (FollowerController, error)

	DeleteShard(req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
}

type shardsDirector struct {
	sync.RWMutex

	config    Config
	leaders   map[int64]LeaderController
	followers map[int64]FollowerController

	kvFactory              kv.Factory
	walFactory             wal.Factory
	replicationRpcProvider ReplicationRpcProvider
	closed                 bool
	log                    *slog.Logger

	leadersCounter   metrics.UpDownCounter
	followersCounter metrics.UpDownCounter
}

func NewShardsDirector(config Config, walFactory wal.Factory, kvFactory kv.Factory, provider ReplicationRpcProvider) ShardsDirector {
	sd := &shardsDirector{
		config:                 config,
		walFactory:             walFactory,
		kvFactory:              kvFactory,
		leaders:                make(map[int64]LeaderController),
		followers:              make(map[int64]FollowerController),
		replicationRpcProvider: provider,
		log: slog.With(
			slog.String("component", "shards-director"),
		),

		leadersCounter: metrics.NewUpDownCounter("oxia_server_leaders_count",
			"The number of leader controllers in a server", "count", map[string]any{}),
		followersCounter: metrics.NewUpDownCounter("oxia_server_followers_count",
			"The number of follower controllers in a server", "count", map[string]any{}),
	}

	return sd
}

func (s *shardsDirector) GetLeader(shardId int64) (LeaderController, error) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, common.ErrorAlreadyClosed
	}

	if leader, ok := s.leaders[shardId]; ok {
		// There is already a leader controller for this shard
		return leader, nil
	}

	s.log.Debug(
		"This node is not hosting shard",
		slog.Int64("shard", shardId),
	)
	return nil, status.Errorf(common.CodeNodeIsNotLeader, "node is not leader for shard %d", shardId)
}

func (s *shardsDirector) GetFollower(shardId int64) (FollowerController, error) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, common.ErrorAlreadyClosed
	}

	if follower, ok := s.followers[shardId]; ok {
		// There is already a follower controller for this shard
		return follower, nil
	}

	s.log.Debug(
		"This node is not hosting shard",
		slog.Int64("shard", shardId),
	)
	return nil, status.Errorf(common.CodeNodeIsNotFollower, "node is not follower for shard %d", shardId)
}

func (s *shardsDirector) GetOrCreateLeader(namespace string, shardId int64) (LeaderController, error) {
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

		if _, ok := s.followers[shardId]; ok {
			s.followersCounter.Dec()
			delete(s.followers, shardId)
		}
	}

	// Create new leader controller
	lc, err := NewLeaderController(s.config, namespace, shardId, s.replicationRpcProvider, s.walFactory, s.kvFactory)
	if err != nil {
		return nil, err
	}

	s.leaders[shardId] = lc
	s.leadersCounter.Inc()
	return lc, nil
}

func (s *shardsDirector) GetOrCreateFollower(namespace string, shardId int64) (FollowerController, error) {
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
		// Let's close it before creating the follower controller

		if err := leader.Close(); err != nil {
			return nil, err
		}

		if _, ok := s.leaders[shardId]; ok {
			s.leadersCounter.Dec()
			delete(s.leaders, shardId)
		}
	}

	// Create new follower controller
	fc, err := NewFollowerController(s.config, namespace, shardId, s.walFactory, s.kvFactory)
	if err != nil {
		return nil, err
	}

	s.followers[shardId] = fc
	s.followersCounter.Inc()
	return fc, nil
}

func (s *shardsDirector) DeleteShard(req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	s.Lock()
	defer s.Unlock()

	if leader, ok := s.leaders[req.ShardId]; ok {
		resp, err := leader.DeleteShard(req)
		if err != nil {
			return nil, err
		}

		delete(s.leaders, req.ShardId)
		s.leadersCounter.Dec()
		return resp, nil
	}

	if follower, ok := s.followers[req.ShardId]; ok {
		resp, err := follower.DeleteShard(req)
		if err != nil {
			return nil, err
		}

		delete(s.followers, req.ShardId)
		s.followersCounter.Dec()
		return resp, nil
	}

	fc, err := NewFollowerController(s.config, req.Namespace, req.ShardId, s.walFactory, s.kvFactory)
	if err != nil {
		return nil, err
	}
	return fc.DeleteShard(req)
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
