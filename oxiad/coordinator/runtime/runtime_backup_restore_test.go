// Copyright 2023-2026 The Oxia Authors
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

package runtime

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
)

// restoreTestCluster is a runtime over one running data server, with a
// backup policy, whose provider lists the backups of the source namespace.
// The named metadata call fails while failing holds its name.
type restoreTestCluster struct {
	runtime  Runtime
	metadata coordmetadata.Metadata
	provider *backupRPCProvider
	failing  atomic.Pointer[string]
}

var errMetadataUnavailable = errors.New("metadata unavailable")

func (c *restoreTestCluster) fails(call string) bool {
	return c.failing.Load() != nil && *c.failing.Load() == call
}

func (c *restoreTestCluster) fail(call string) {
	c.failing.Store(&call)
}

func newRestoreTestCluster(t *testing.T, backups []*proto.BackupInfo) *restoreTestCluster {
	t.Helper()
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: constant.DefaultNamespace, InitialShardCount: 1, ReplicationFactor: 1}},
		Servers:    []*proto.DataServerIdentity{server},
		Backup:     &proto.BackupConfig{Enabled: true, Destination: "s3://bucket/prefix"},
	})
	// A data server is tracked once a shard is assigned to it
	_, err := metadata.AllocateShardIDs(1)
	require.NoError(t, err)
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{0: {
			Status:         proto.ShardStatusSteadyState,
			Term:           1,
			Leader:         server,
			Ensemble:       []*proto.DataServerIdentity{server},
			Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
		}},
	}))
	c := &restoreTestCluster{metadata: metadata}
	failing := &fakeMetadata{
		Metadata: metadata,
		getNamespace: func(name string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
			if c.fails("GetNamespace") {
				return commonobject.Borrowed[*proto.Namespace]{}, false, errMetadataUnavailable
			}
			return metadata.GetNamespace(name)
		},
		getNamespaceStatus: func(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
			if c.fails("GetNamespaceStatus") {
				return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, errMetadataUnavailable
			}
			return metadata.GetNamespaceStatus(namespace)
		},
		allocateShardIDs: func(count uint32) (int64, error) {
			if c.fails("AllocateShardIDs") {
				return 0, errMetadataUnavailable
			}
			return metadata.AllocateShardIDs(count)
		},
		listNamespaceStatus: func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
			if c.fails("ListNamespaceStatus") {
				return nil, errMetadataUnavailable
			}
			return metadata.ListNamespaceStatus()
		},
		createNamespaceStatus: func(namespace string, status *proto.NamespaceStatus) error {
			if c.fails("CreateNamespaceStatus") {
				return errMetadataUnavailable
			}
			return metadata.CreateNamespaceStatus(namespace, status)
		},
		createNamespace: func(namespace *proto.Namespace) error {
			if c.fails("CreateNamespace") {
				return errMetadataUnavailable
			}
			return metadata.CreateNamespace(namespace)
		},
	}
	c.provider = &backupRPCProvider{RpcProvider: rpctest.NewRpcProvider(), backups: backups}
	r, err := New(failing, func(string) rpc.Provider { return c.provider })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	require.Eventually(t, func() bool {
		status, ok := r.GetDataServerStatus("s1")
		return ok && status.GetState() == proto.DataServerState_DATA_SERVER_STATE_RUNNING
	}, 10*time.Second, 10*time.Millisecond)
	c.runtime = r
	return c
}

func restoreRequest(now time.Time) *proto.RestoreNamespaceRequest {
	return &proto.RestoreNamespaceRequest{SourceNamespace: "orders", TargetNamespace: "orders-restored",
		AsOfMillis: now.UnixMilli()}
}

func TestRuntimeRestoreNamespaceCreatesTheShardsFromTheBackups(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	config := &proto.Namespace{Name: "orders", ReplicationFactor: 1}
	// The newer backup of shard 2 covers the right half; the left half comes
	// from the older backup of shard 1
	older, newer := backupInfo(1, now.Add(-2*time.Hour), 0, math.MaxUint32, config), backupInfo(2, now.Add(-time.Hour), 1<<31, math.MaxUint32, config)
	c := newRestoreTestCluster(t, []*proto.BackupInfo{older, newer})

	res, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	require.NoError(t, err)
	require.Len(t, res.GetShards(), 2)
	assert.EqualValues(t, 1, res.GetShards()[0].GetSourceShard())
	assert.Equal(t, older.GetBackupId(), res.GetShards()[0].GetBackupId())
	assert.EqualValues(t, 1<<31-1, res.GetShards()[0].GetHashRange().GetMax())
	assert.EqualValues(t, 2, res.GetShards()[1].GetSourceShard())
	assert.EqualValues(t, 1<<31, res.GetShards()[1].GetHashRange().GetMin())
	assert.NotEqual(t, res.GetShards()[0].GetShard(), res.GetShards()[1].GetShard())

	borrowed, exists, err := c.metadata.GetNamespace("orders-restored")
	require.NoError(t, err)
	require.True(t, exists, "the namespace is created last")
	assert.EqualValues(t, 2, borrowed.UnsafeBorrow().GetInitialShardCount())
	status, exists, err := c.metadata.GetNamespaceStatus("orders-restored")
	require.NoError(t, err)
	require.True(t, exists)
	for _, shard := range res.GetShards() {
		meta := status.UnsafeBorrow().GetShards()[shard.GetShard()]
		require.NotNil(t, meta)
		assert.Equal(t, "s3://bucket/prefix", meta.GetRestore().GetDestination(), "the destination of the policy")
		assert.Equal(t, "orders", meta.GetRestore().GetSourceNamespace())
		assert.Equal(t, shard.GetBackupId(), meta.GetRestore().GetBackupId())
	}
	c.provider.mu.Lock()
	assert.Equal(t, "orders", c.provider.listReq.GetNamespace())
	assert.Nil(t, c.provider.listReq.Shard, "every shard of the source is listed")
	assert.Equal(t, now.UnixMilli(), c.provider.listReq.GetNewestAtOrBeforeMillis(), "one backup per shard, as of the time")
	c.provider.mu.Unlock()

	// The namespace exists now
	_, err = c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, metadatacommon.ErrAlreadyExists)
}

func TestRuntimeRestoreNamespaceResumesAnInterruptedRestore(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	info := backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32, &proto.Namespace{Name: "orders", ReplicationFactor: 1})
	c := newRestoreTestCluster(t, []*proto.BackupInfo{info})
	// A restore that crashed after creating the status, before the namespace
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	require.NoError(t, c.metadata.CreateNamespaceStatus("orders-restored", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{7: {
			Status:         proto.ShardStatusUnknown,
			Term:           -1,
			Ensemble:       []*proto.DataServerIdentity{server},
			Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
			Restore: &proto.RestoreMetadata{Destination: "s3://bucket/prefix", SourceNamespace: "orders",
				SourceShardId: 1, BackupId: info.GetBackupId()},
		}},
	}))

	res, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	require.NoError(t, err)
	require.Len(t, res.GetShards(), 1)
	assert.EqualValues(t, 7, res.GetShards()[0].GetShard(), "the shard of the interrupted restore")
	_, exists, err := c.metadata.GetNamespace("orders-restored")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestRuntimeRestoreNamespaceRefusesAStatusThatIsNotARestore(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	info := backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32, &proto.Namespace{Name: "orders", ReplicationFactor: 1})
	c := newRestoreTestCluster(t, []*proto.BackupInfo{info})
	require.NoError(t, c.metadata.CreateNamespaceStatus("orders-restored", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards:            map[int64]*proto.ShardMetadata{7: {Status: proto.ShardStatusSteadyState, Term: 1}},
	}))

	_, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, metadatacommon.ErrAlreadyExists)
}

func TestRuntimeRestoreNamespaceNeedsTheBackupService(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Backup: &proto.BackupConfig{Enabled: true, Destination: "s3://bucket/prefix"},
	})
	r, err := New(metadata, func(string) rpc.Provider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	_, err = r.RestoreNamespace(context.Background(), restoreRequest(time.Now()))
	assert.ErrorIs(t, err, ErrBackupsUnsupported)
}

func TestRuntimeRestoreNamespaceReportsTheFailures(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	info := backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32, &proto.Namespace{Name: "orders", ReplicationFactor: 1})
	c := newRestoreTestCluster(t, []*proto.BackupInfo{info})

	// The listing fails
	c.provider.mu.Lock()
	c.provider.listErr = errors.New("bucket unavailable")
	c.provider.mu.Unlock()
	_, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorContains(t, err, "list the backups through s1")
	// No backup is old enough
	c.provider.mu.Lock()
	c.provider.listErr = nil
	c.provider.mu.Unlock()
	_, err = c.runtime.RestoreNamespace(context.Background(), restoreRequest(now.Add(-2*time.Hour)))
	assert.ErrorIs(t, err, backup.ErrNoBackup)
	// The ensemble of a restored shard can't be selected
	c.provider.mu.Lock()
	c.provider.backups = []*proto.BackupInfo{backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32,
		&proto.Namespace{Name: "orders", ReplicationFactor: 3})}
	c.provider.mu.Unlock()
	_, err = c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, selector.ErrUnsatisfiedEnsembleReplicas)
	c.provider.mu.Lock()
	c.provider.backups = []*proto.BackupInfo{info}
	c.provider.mu.Unlock()

	// The metadata fails
	for _, call := range []string{"GetNamespace", "GetNamespaceStatus", "AllocateShardIDs", "ListNamespaceStatus",
		"CreateNamespaceStatus", "CreateNamespace"} {
		t.Run(call, func(t *testing.T) {
			c.fail(call)
			_, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
			assert.ErrorIs(t, err, errMetadataUnavailable)
		})
	}
}
