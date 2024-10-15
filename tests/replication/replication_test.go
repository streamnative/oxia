package replication

import (
	"fmt"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/server"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestReplication_OverrideWrongVersionId(t *testing.T) {
	s1, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})
	assert.NoError(t, err)
	s1Addr := model.ServerAddress{
		Public:   fmt.Sprintf("localhost:%d", s1.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s1.InternalPort()),
	}
	defer s1.Close()
	s2, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})
	assert.NoError(t, err)
	s2Addr := model.ServerAddress{
		Public:   fmt.Sprintf("localhost:%d", s2.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s2.InternalPort()),
	}
	defer s2.Close()
	s3, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})
	assert.NoError(t, err)
	s3Addr := model.ServerAddress{
		Public:   fmt.Sprintf("localhost:%d", s3.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s3.InternalPort()),
	}
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{s1Addr, s2Addr, s3Addr},
	}

	clientPool := common.NewClientPool(nil, nil)

	coordinator, err := impl.NewCoordinator(metadataProvider,
		func() (model.ClusterConfig, error) { return clusterConfig, nil },
		nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	defer coordinator.Close()

	clusterStatus, _, err := metadataProvider.Get()
	assert.NoError(t, err)

	status := clusterStatus.Namespaces[common.DefaultNamespace]
	metadata := status.Shards[1]
	print(metadata.Term)
}
