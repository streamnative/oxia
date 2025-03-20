package server

import (
	"context"
	"fmt"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

func init() {
	common.PprofEnable = true
	common.PprofBindAddress = "127.0.0.1:6060"
	common.ConfigureLogger()
}

func Test_DB_DeleteRange(t *testing.T) {
	factory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 2048,
		InMemory:    true,
	})
	assert.NoError(t, err)
	shard := int64(1)

	dir, err := os.Getwd()
	assert.NoError(t, err)
	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  path.Join(dir, "testdata", "rangedelete", "wal"),
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
