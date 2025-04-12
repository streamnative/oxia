// Copyright 2025 StreamNative, Inc.
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

package checkpoint

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/policies"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/tests/utils"
)

func TestCheckpoint_VersionID(t *testing.T) {
	for _, test := range []struct {
		name            string
		failureHandling int32
	}{
		{name: "checkpoint-version-id-failure-handling-warn", failureHandling: policies.FailureHandlingWarn},
		{name: "checkpoint-version-id-failure-handling-discard", failureHandling: policies.FailureHandlingDiscard},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkpointEnabled := true
			checkpointCommitEvery := int32(5)
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
								FailureHandling: &test.failureHandling,
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

			dirtyCounter := 2
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
				puts := make([]*proto.PutRequest, 0)
				for i := range dirtyCounter {
					puts = append(puts, &proto.PutRequest{
						Key:   fmt.Sprintf("dirty-%v", i),
						Value: []byte("dirty"),
					})
				}
				dirtyCounter += 3
				_, err = db.ProcessWrite(&proto.WriteRequest{
					Puts: puts,
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
			_, _, err = client.Put(context.Background(), "t11", []byte("t11"))
			assert.NoError(t, err)
			_, _, err = client.Put(context.Background(), "t12", []byte("t12"))
			assert.NoError(t, err)

			lc, leaderInfo = utils.GetClusterLeader(t, coordinator, servers, common.DefaultNamespace, 0)
			lcDB = lc.GetDB()
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
				switch test.failureHandling {
				case policies.FailureHandlingWarn:
					assert.Never(t, func() bool {
						fcLastVersion, err := db.ReadLastVersionId()
						if err != nil {
							return false
						}
						return lcLastVersion-1 == fcLastVersion
					}, time.Second, time.Millisecond*100)
					break
				case policies.FailureHandlingDiscard:
					assert.Eventually(t, func() bool {
						fcLastVersion, err := db.ReadLastVersionId()
						if err != nil {
							return false
						}
						return lcLastVersion-1 == fcLastVersion
					}, time.Second, time.Millisecond*100)
					break
				}
			}
		})
	}
}
