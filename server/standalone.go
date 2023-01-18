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
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/common/container"
	"oxia/common/metrics"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
)

type StandaloneConfig struct {
	Config

	BindHost string

	AdvertisedPublicAddress string
	NumShards               uint32
	InMemory                bool
}

type Standalone struct {
	rpc                       *publicRpcServer
	kvFactory                 kv.KVFactory
	walFactory                wal.WalFactory
	shardsDirector            ShardsDirector
	shardAssignmentDispatcher ShardAssignmentsDispatcher

	metrics *metrics.PrometheusMetrics
}

func NewTestConfig() StandaloneConfig {
	return StandaloneConfig{
		NumShards:               1,
		BindHost:                "localhost",
		AdvertisedPublicAddress: "localhost",
		InMemory:                true,
	}
}

func NewStandalone(config StandaloneConfig) (*Standalone, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia standalone")

	s := &Standalone{}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		advertisedPublicAddress = hostname
	}

	var kvOptions kv.KVFactoryOptions
	if config.InMemory {
		kvOptions = kv.KVFactoryOptions{InMemory: true}
		s.walFactory = wal.NewInMemoryWalFactory()
	} else {
		kvOptions = kv.KVFactoryOptions{DataDir: config.DataDir}
		s.walFactory = wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: config.WalDir})
	}
	var err error
	if s.kvFactory, err = kv.NewPebbleKVFactory(&kvOptions); err != nil {
		return nil, err
	}

	s.shardsDirector = NewShardsDirector(config.Config, s.walFactory, s.kvFactory, newNoOpReplicationRpcProvider())

	if err := s.initializeShards(config.NumShards); err != nil {
		return nil, err
	}

	bindAddress := fmt.Sprintf("%s:%d", config.BindHost, config.PublicServicePort)
	s.rpc, err = newPublicRpcServer(container.Default, bindAddress, s.shardsDirector, nil)
	if err != nil {
		return nil, err
	}

	s.shardAssignmentDispatcher = NewStandaloneShardAssignmentDispatcher(
		fmt.Sprintf("%s:%d", advertisedPublicAddress, s.rpc.Port()),
		config.NumShards)

	s.rpc.assignmentDispatcher = s.shardAssignmentDispatcher

	s.metrics, err = metrics.Start(fmt.Sprintf("%s:%d", config.BindHost, config.MetricsPort))
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Standalone) initializeShards(numShards uint32) error {
	var err error
	for i := uint32(0); i < numShards; i++ {
		var lc LeaderController
		if lc, err = s.shardsDirector.GetOrCreateLeader(i); err != nil {
			return err
		}

		newEpoch := lc.Epoch() + 1

		if _, err := lc.Fence(&proto.FenceRequest{
			ShardId: i,
			Epoch:   newEpoch,
		}); err != nil {
			return err
		}

		if _, err := lc.BecomeLeader(&proto.BecomeLeaderRequest{
			ShardId:           i,
			Epoch:             newEpoch,
			ReplicationFactor: 1,
			FollowerMaps:      make(map[string]*proto.EntryId),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Standalone) RpcPort() int {
	return s.rpc.Port()
}

func (s *Standalone) Close() error {
	return multierr.Combine(
		s.shardsDirector.Close(),
		s.shardAssignmentDispatcher.Close(),
		s.rpc.Close(),
		s.kvFactory.Close(),
		s.metrics.Close(),
	)
}

///////////////////////////////////

type noOpReplicationRpcProvider struct {
}

func (n noOpReplicationRpcProvider) Close() error {
	return nil
}

func (n noOpReplicationRpcProvider) GetReplicateStream(ctx context.Context, follower string, shard uint32) (proto.OxiaLogReplication_ReplicateClient, error) {
	panic("not implemented")
}

func (n noOpReplicationRpcProvider) SendSnapshot(ctx context.Context, follower string, shard uint32) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	panic("not implemented")
}

func (n noOpReplicationRpcProvider) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	panic("not implemented")
}

func newNoOpReplicationRpcProvider() ReplicationRpcProvider {
	return &noOpReplicationRpcProvider{}
}
