package impl

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
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
		BindHost:            "localhost",
		PublicServicePort:   0,
		InternalServicePort: 0,
		MetricsPort:         -1, // Disable metrics to avoid conflict
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
		ReplicationFactor: 3,
		ShardCount:        1,
		Servers:           []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, clusterConfig, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	assert.EqualValues(t, 3, coordinator.ClusterStatus().ReplicationFactor)
	assert.EqualValues(t, 1, len(coordinator.ClusterStatus().Shards))

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

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
		ReplicationFactor: 3,
		ShardCount:        1,
		Servers:           []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, clusterConfig, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	assert.EqualValues(t, 3, coordinator.ClusterStatus().ReplicationFactor)
	assert.EqualValues(t, 1, len(coordinator.ClusterStatus().Shards))

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	cs := coordinator.ClusterStatus()

	leader := *cs.Shards[0].Leader
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

	clientOptions, err := oxia.NewClientOptions(follower.Public)
	assert.NoError(t, err)
	client := oxia.NewSyncClient(clientOptions)

	stat1, err := client.Put("my-key", []byte("my-value"), nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, stat1.Version)

	res, stat2, err := client.Get("my-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, stat1, stat2)
	assert.NoError(t, client.Close())

	// Stop the leader to cause a leader election
	assert.NoError(t, servers[leader].Close())
	delete(servers, leader)

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	// Wait for the client to receive the updated assignment list
	assert.Eventually(t, func() bool {
		client = oxia.NewSyncClient(clientOptions)
		_, _, err := client.Get("my-key")
		return err == nil
	}, 10*time.Second, 10*time.Millisecond)

	res, stat3, err := client.Get("my-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, stat1, stat3)
	assert.NoError(t, client.Close())

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	for _, server := range servers {
		assert.NoError(t, server.Close())
	}
}
