package coordinator

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/server"
	"testing"
	"time"
)

func newServer(t *testing.T) (s *server.Server, addr ServerAddress) {
	var err error
	s, err = server.New(server.Config{
		PublicServicePort:   0,
		InternalServicePort: 0,
		MetricsPort:         -1, // Disable metrics to avoid conflict
		DataDir:             t.TempDir(),
		WalDir:              t.TempDir(),
	})

	assert.NoError(t, err)

	addr = ServerAddress{
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
	clusterConfig := ClusterConfig{
		ReplicationFactor: 3,
		ShardsCount:       1,
		StorageServers:    []ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, clusterConfig, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	assert.EqualValues(t, 3, coordinator.ClusterStatus().ReplicationFactor)
	assert.EqualValues(t, 1, len(coordinator.ClusterStatus().Shards))

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Shards[0]
		return shard.Status == ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}
