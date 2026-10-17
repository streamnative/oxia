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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/proto"
)

func newTestWalFactory(t *testing.T) wal.Factory {
	t.Helper()

	return wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  t.TempDir(),
		SegmentSize: 128 * 1024,
	})
}

func TestShardsDirector_DeleteShardLeader(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	sd := NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient())

	lc, _ := sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: constant.DefaultNamespace,
		Shard:     shard,
		Term:      1,
	})
	assert.NoError(t, err)

	// Reopen
	lc, err = sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestShardsDirector_GetOrCreateFollower(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	sd := NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient())

	lc, _ := sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	assert.EqualValues(t, 2, lc.Term())

	// Should fail to get closed if the term is wrong
	fc, err := sd.GetOrCreateFollower(constant.DefaultNamespace, shard, 1, nil)
	assert.ErrorIs(t, constant.ErrInvalidTerm, err)
	assert.Nil(t, fc)
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	// Will get closed if term is correct
	fc, err = sd.GetOrCreateFollower(constant.DefaultNamespace, shard, 2, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestShardsDirectorRoutesCheckpointAndCommitOffsetToTheHostingController(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	sd := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	t.Cleanup(func() { assert.NoError(t, sd.Close()) })

	// A shard led here, with two entries
	var led int64 = 1
	lc, err := sd.GetOrCreateLeader(constant.DefaultNamespace, led, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: led, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: led, Term: 1, ReplicationFactor: 1})
	require.NoError(t, err)
	for _, key := range []string{"a", "b"} {
		_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
			Shard: &led, Puts: []*proto.PutRequest{{Key: key, Value: []byte("v")}}})
		require.NoError(t, err)
	}
	offset, err := sd.CommitOffset(led)
	require.NoError(t, err)
	assert.EqualValues(t, 1, offset)
	checkpoint, err := sd.Checkpoint(led)
	require.NoError(t, err)
	commitOffset, _, err := database.ReadCheckpoint(checkpoint.BasePath())
	require.NoError(t, err)
	assert.EqualValues(t, 1, commitOffset)
	assert.NoError(t, checkpoint.Close())
	require.NoError(t, sd.WithDatabase(led, checkpoint.Generation, func(dir string) error {
		assert.Equal(t, sd.DatabaseDir(constant.DefaultNamespace, led), dir)
		return nil
	}))

	// A shard followed here, with no entry yet
	var followed int64 = 2
	fc, err := sd.GetOrCreateFollower(constant.DefaultNamespace, followed, 1, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Shard: followed, Term: 1})
	require.NoError(t, err)
	offset, err = sd.CommitOffset(followed)
	require.NoError(t, err)
	assert.EqualValues(t, wal.InvalidOffset, offset)
	checkpoint, err = sd.Checkpoint(followed)
	require.NoError(t, err)
	assert.NoError(t, checkpoint.Close())
	require.NoError(t, sd.WithDatabase(followed, checkpoint.Generation, func(string) error { return nil }))

	// A shard not hosted here
	var absent int64 = 3
	_, err = sd.CommitOffset(absent)
	assert.ErrorIs(t, err, constant.ErrNodeIsNotMember)
	_, err = sd.Checkpoint(absent)
	assert.ErrorIs(t, err, constant.ErrNodeIsNotMember)
	assert.ErrorIs(t, sd.WithDatabase(absent, 0, func(string) error { return nil }), constant.ErrNodeIsNotMember)
}
