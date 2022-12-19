package impl

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/coordinator/model"
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
