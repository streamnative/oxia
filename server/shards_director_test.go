// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
)

func TestShardsDirector_DeleteShardLeader(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := wal.NewInMemoryWalFactory()

	sd := NewShardsDirector(Config{}, walFactory, kvFactory, newMockRpcClient())

	lc, _ := sd.GetOrCreateLeader(common.DefaultNamespace, shard)
	_, _ = lc.NewTerm(&proto.NewTermRequest{ShardId: shard, Term: 1})
	_, _ = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(&proto.WriteRequest{
		ShardId: &shard,
		Puts:    []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: common.DefaultNamespace,
		ShardId:   shard,
		Term:      1,
	})
	assert.NoError(t, err)

	// Reopen
	lc, err = sd.GetOrCreateLeader(common.DefaultNamespace, shard)
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}
