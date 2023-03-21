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

package impl

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"math"
	"oxia/common"
	"oxia/coordinator/model"
	"oxia/oxia"
	"oxia/server"
	"testing"
	"time"
)

func newServer(t *testing.T) (s *server.Server, addr model.ServerAddress) {
	var err error
	s, err = server.New(server.Config{
		PublicServiceAddr:   "localhost:0",
		InternalServiceAddr: "localhost:0",
		MetricsServiceAddr:  "", // Disable metrics to avoid conflict
		DataDir:             t.TempDir(),
		WalDir:              t.TempDir(),
	})

	assert.NoError(t, err)

	addr = model.ServerAddress{
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func TestCoordinatorE2E(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, clusterConfig, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, len(coordinator.ClusterStatus().Namespaces))
	nsStatus := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinatorE2E_ShardsRanges(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 4,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, clusterConfig, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	cs := coordinator.ClusterStatus()
	nsStatus := cs.Namespaces[common.DefaultNamespace]
	assert.EqualValues(t, 4, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	// Check that the entire hash range is covered
	assert.EqualValues(t, 0, nsStatus.Shards[0].Int32HashRange.Min)

	for i := uint32(1); i < 4; i++ {
		log.Info().
			Interface("range", nsStatus.Shards[i].Int32HashRange).
			Uint32("shard", i).
			Msg("Checking shard")

		// The hash ranges should be exclusive & consecutive
		assert.Equal(t, nsStatus.Shards[i-1].Int32HashRange.Max+1, nsStatus.Shards[i].Int32HashRange.Min)
	}

	assert.EqualValues(t, math.MaxUint32, nsStatus.Shards[3].Int32HashRange.Max)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinator_LeaderFailover(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.ServerAddress]*server.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, clusterConfig, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	nsStatus := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	cs := coordinator.ClusterStatus()
	nsStatus = cs.Namespaces[common.DefaultNamespace]

	leader := *nsStatus.Shards[0].Leader
	var follower model.ServerAddress
	for server := range servers {
		if server != leader {
			follower = server
			break
		}
	}
	log.Logger.Info().
		Interface("leader", leader).
		Interface("follower", follower).
		Msg("Cluster is ready")

	client, err := oxia.NewSyncClient(follower.Public)
	assert.NoError(t, err)

	ctx := context.Background()

	version1, err := client.Put(ctx, "my-key", []byte("my-value"))
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.VersionId)

	res, version2, err := client.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, version1, version2)
	assert.NoError(t, client.Close())

	// Stop the leader to cause a leader election
	assert.NoError(t, servers[leader].Close())
	delete(servers, leader)

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	// Wait for the client to receive the updated assignment list
	assert.Eventually(t, func() bool {
		client, _ = oxia.NewSyncClient(follower.Public)
		_, _, err := client.Get(ctx, "my-key")
		return err == nil
	}, 10*time.Second, 10*time.Millisecond)

	res, version3, err := client.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, version1, version3)
	assert.NoError(t, client.Close())

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	for _, server := range servers {
		assert.NoError(t, server.Close())
	}
}
