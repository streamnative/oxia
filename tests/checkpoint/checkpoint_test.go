package checkpoint

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/policies"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server"
	"github.com/streamnative/oxia/server/kv"
	"github.com/stretchr/testify/assert"
)

func TestCheckpoint_VersionID(t *testing.T) {

	servers := map[string]*server.Server{}
	s1, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})
	assert.NoError(t, err)
	s1Addr := model.Server{
		Public:   fmt.Sprintf("localhost:%d", s1.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s1.InternalPort()),
	}
	servers[s1Addr.GetIdentifier()] = s1

	s2, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})
	assert.NoError(t, err)
	s2Addr := model.Server{
		Public:   fmt.Sprintf("localhost:%d", s2.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s2.InternalPort()),
	}
	servers[s2Addr.GetIdentifier()] = s2

	s3, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})
	assert.NoError(t, err)
	s3Addr := model.Server{
		Public:   fmt.Sprintf("localhost:%d", s3.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s3.InternalPort()),
	}
	servers[s3Addr.GetIdentifier()] = s3

	metadataProvider := impl.NewMetadataProviderMemory()
	checkpointEnabled := true
	checkpointCommitEvery := int64(5)
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
			Policies: &policies.Policies{
				Checkpoint: &policies.Checkpoint{
					Enabled:     &checkpointEnabled,
					CommitEvery: &checkpointCommitEvery,
				},
			},
		}},
		Servers: []model.Server{s1Addr, s2Addr, s3Addr},
	}
	clientPool := common.NewClientPool(nil, nil)
	coordinator, err := impl.NewCoordinator(metadataProvider,
		func() (model.ClusterConfig, error) { return clusterConfig, nil },
		nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	client, err := oxia.NewSyncClient(s1Addr.Public)
	assert.NoError(t, err)

	defer client.Close()

	_, _, err = client.Put(context.Background(), "t1", []byte("t1"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t2", []byte("t2"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t3", []byte("t3"))
	assert.NoError(t, err)

	status := coordinator.ClusterStatus()
	namespaceStatus := status.Namespaces[common.DefaultNamespace]
	firstShare := namespaceStatus.Shards[0]

	leaderServer := firstShare.Leader
	leader := servers[leaderServer.GetIdentifier()]
	lc, err := leader.GetShardsDirector().GetLeader(0)
	assert.NoError(t, err)
	lcDB := lc.GetDB()
	lcCommitOffset, err := lcDB.ReadCommitOffset()
	assert.NoError(t, err)
	lcLastVersion, err := lcDB.ReadLastVersionId()
	assert.NoError(t, err)

	for svID, sv := range servers {
		if svID == leaderServer.GetIdentifier() {
			continue
		}
		follower := sv
		fd := follower.GetShardsDirector()
		f, err := fd.GetFollower(0)
		assert.NoError(t, err)
		db := f.GetDB()
		fcLastVersion, err := db.ReadLastVersionId()
		assert.NoError(t, err)
		assert.Equal(t, lcLastVersion-1, fcLastVersion)

		// make the follower become dirty
		_, err = db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{
				{
					Key:   "dirty",
					Value: []byte("dirty"),
				},
			},
		}, lcCommitOffset, 0, kv.NoOpCallback)
		assert.NoError(t, err)

		// last version has already dirty
		fcLastVersion, err = db.ReadLastVersionId()
		assert.NoError(t, err)
		assert.NotEqual(t, lcLastVersion-1, fcLastVersion)
	}

	_, _, err = client.Put(context.Background(), "t4", []byte("t4"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t5", []byte("t5"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t6", []byte("t6"))
	assert.NoError(t, err)

	// trigger election
	coordinator.NodeBecameUnavailable(*leaderServer)
	_, _, err = client.Put(context.Background(), "t7", []byte("t7"))
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)
}
