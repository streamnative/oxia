package replication

import (
	"context"
	"fmt"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/server"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"testing"
	"time"
)

type SvWithAddr struct {
	server *server.Server
	addr   model.ServerAddress
}

func TestReplication_OverrideWrongVersionId(t *testing.T) {
	currentContext := context.Background()
	common.LogLevel = slog.LevelDebug
	common.ConfigureLogger()

	clusterContext := startOxiaCluster(t)

	assert.Eventually(t, func() bool {
		clusterStatus, _, err := clusterContext.metaProvider.Get()
		assert.NoError(t, err)

		status := clusterStatus.Namespaces[common.DefaultNamespace]
		metadata := status.Shards[0]
		return metadata.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	// avoid changes
	clusterContext.coordinator.Close()

	client, err := oxia.NewSyncClient(clusterContext.servers[0].addr.Public)
	assert.NoError(t, err)
	_, version, err := client.Put(currentContext, "key-1", []byte{})
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version.VersionId)

	_, version, err = client.Put(currentContext, "key-2", []byte{})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, version.VersionId)

	// inject error
	clusterStatus, _, err := clusterContext.metaProvider.Get()
	assert.NoError(t, err)
	status := clusterStatus.Namespaces[common.DefaultNamespace]
	shardMetadata := status.Shards[0]

	// simulate upgrade breaking versions
	for _, sv := range clusterContext.servers {
		if sv.addr.Public == shardMetadata.Leader.Public {
			// skip leader
			continue
		}
		director := sv.server.GetShardDirector()
		follower, err := director.GetFollower(0)
		assert.NoError(t, err)
		db := follower.GetDB()
		assert.Eventually(t, func() bool {
			offset, err := db.ReadCommitOffset()
			assert.NoError(t, err)
			return offset == 2
		}, 60*time.Second, 100*time.Millisecond)
		err = db.AddASIILong(common.InternalKeyPrefix+"last-version-id", 0, uint64(time.Now().Unix()))
		assert.NoError(t, err)
	}

	_, version, err = client.Put(currentContext, "key-3", []byte{})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, version.VersionId)

	_, version, err = client.Put(currentContext, "key-4", []byte{})
	assert.NoError(t, err)
	assert.EqualValues(t, 3, version.VersionId)

	for _, sv := range clusterContext.servers {
		if sv.addr.Public == shardMetadata.Leader.Public {
			// skip leader
			continue
		}
		director := sv.server.GetShardDirector()
		follower, err := director.GetFollower(0)
		assert.NoError(t, err)
		db := follower.GetDB()
		id, err := db.ReadLastVersionId()
		assert.NoError(t, err)
		assert.EqualValues(t, version.VersionId+1, id)
	}
}

type OxiaClusterContext struct {
	servers      []SvWithAddr
	coordinator  impl.Coordinator
	metaProvider impl.MetadataProvider
}

func startOxiaCluster(t *testing.T) *OxiaClusterContext {
	t.Helper()
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

	return &OxiaClusterContext{
		servers: []SvWithAddr{
			{
				server: s1,
				addr:   s1Addr,
			},
			{
				server: s2,
				addr:   s2Addr,
			},
			{
				server: s3,
				addr:   s3Addr,
			},
		},
		coordinator:  coordinator,
		metaProvider: metadataProvider,
	}
}
