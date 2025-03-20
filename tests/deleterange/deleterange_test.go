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

package deleterange

import (
	"context"
	"fmt"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

func Test_DeleteRange(t *testing.T) {
	factory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 2048,
	})
	assert.NoError(t, err)
	shard := int64(1)

	dir, err := os.Getwd()
	assert.NoError(t, err)
	lc, err := server.NewLeaderController(server.Config{}, common.DefaultNamespace, shard, nil, wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  path.Join(dir, "testdata", "wal"),
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
	}), factory)
	assert.NoError(t, err)
	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())
	defer lc.Close()

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  1,
	})
	assert.NoError(t, err)

	start := time.Now()
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      map[string]*proto.EntryId{},
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Term())
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())
	println(fmt.Sprintf("cost %d to do recovery", time.Since(start).Milliseconds()))
}
