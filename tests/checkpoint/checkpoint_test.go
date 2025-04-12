package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/policies"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/tests/utils"
	"github.com/stretchr/testify/assert"
)

func TestCheckpoint_VersionID(t *testing.T) {

	checkpointEnabled := true
	checkpointCommitEvery := int32(5)
	failureHandling := policies.FailureHandlingDiscard
	coordinator, serverInfos, servers, cleanupFunc := utils.CreateCluster(t, utils.TestClusterOptions{
		ServerNum: 3,
		ClusterConfig: model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:              common.DefaultNamespace,
				ReplicationFactor: 3,
				InitialShardCount: 1,
				Policies: &policies.Policies{
					Checkpoint: &policies.Checkpoint{
						Enabled:         &checkpointEnabled,
						CommitEvery:     &checkpointCommitEvery,
						FailureHandling: &failureHandling,
					},
				},
			}},
		},
	})

	defer cleanupFunc()

	client, err := oxia.NewSyncClient(serverInfos[0].Public)
	assert.NoError(t, err)
	defer client.Close()

	_, _, err = client.Put(context.Background(), "t1", []byte("t1"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t2", []byte("t2"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t3", []byte("t3"))
	assert.NoError(t, err)

	lc, leaderInfo := utils.GetClusterLeader(t, coordinator, servers, common.DefaultNamespace, 0)
	lcDB := lc.GetDB()
	lcCommitOffset, err := lcDB.ReadCommitOffset()
	assert.NoError(t, err)
	lcLastVersion, err := lcDB.ReadLastVersionId()
	assert.NoError(t, err)

	for svID, sv := range servers {
		if svID == leaderInfo.GetIdentifier() {
			continue
		}
		follower := sv
		fd := follower.GetShardsDirector()
		f, err := fd.GetFollower(0)
		assert.NoError(t, err)
		db := f.GetDB()
		assert.Eventually(t, func() bool {
			fcLastVersion, err := db.ReadLastVersionId()
			if err != nil {
				return false
			}
			return lcLastVersion-1 == fcLastVersion
		}, time.Second, time.Millisecond*100)

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
		fcLastVersion, err := db.ReadLastVersionId()
		assert.NoError(t, err)
		assert.NotEqual(t, lcLastVersion-1, fcLastVersion)
	}

	_, _, err = client.Put(context.Background(), "t4", []byte("t4"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t5", []byte("t5"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t6", []byte("t6"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t7", []byte("t7"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "t8", []byte("t8"))
	assert.NoError(t, err)

	// trigger election
	coordinator.NodeBecameUnavailable(*leaderInfo)

	time.Sleep(2 * time.Second)
	assert.Eventually(t, func() bool {
		status := coordinator.ClusterStatus()
		namespaceStatus := status.Namespaces[common.DefaultNamespace]
		firstShare := namespaceStatus.Shards[0]
		return firstShare.Status == model.ShardStatusSteadyState
	}, time.Second, time.Millisecond*100)

	_, _, err = client.Put(context.Background(), "t9", []byte("t9"))
	assert.NoError(t, err)

	_, _, err = client.Put(context.Background(), "t10", []byte("t10"))
	assert.NoError(t, err)

	lc, leaderInfo = utils.GetClusterLeader(t, coordinator, servers, common.DefaultNamespace, 0)
	lcDB = lc.GetDB()
	lcCommitOffset, err = lcDB.ReadCommitOffset()
	assert.NoError(t, err)
	lcLastVersion, err = lcDB.ReadLastVersionId()
	assert.NoError(t, err)

	for svID, sv := range servers {
		if svID == leaderInfo.GetIdentifier() {
			continue
		}
		follower := sv
		fd := follower.GetShardsDirector()
		f, err := fd.GetFollower(0)
		assert.NoError(t, err)
		db := f.GetDB()
		assert.Eventually(t, func() bool {
			fcLastVersion, err := db.ReadLastVersionId()
			if err != nil {
				return false
			}
			return lcLastVersion-1 == fcLastVersion
		}, time.Second, time.Millisecond*100)
	}
}
