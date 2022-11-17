package client

import (
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/server/kv"
	"oxia/standalone"
	"strconv"
	"testing"
)

type testShardStrategy struct {
}

func (s *testShardStrategy) Get(key string) func(Shard) bool {
	return func(shard Shard) bool {
		return true
	}
}

func TestWithStandalone(t *testing.T) {
	port := GetFreePort()

	kvOptions := kv.KVFactoryOptions{InMemory: true}
	kvFactory := kv.NewPebbleKVFactory(&kvOptions)
	_, err := standalone.NewStandaloneRpcServer(port, "localhost", 2, kvFactory)
	assert.ErrorIs(t, nil, err)

	clientPool := common.NewClientPool()
	serviceUrl := "localhost:" + strconv.FormatInt(int64(port), 10)
	shardManager := NewShardManager(&testShardStrategy{}, clientPool, serviceUrl).(*shardManagerImpl)
	defer func() {
		if err := shardManager.Close(); err != nil {
			assert.Fail(t, "could not close shard manager")
		}
	}()

	shardManager.Start()

	shardId := shardManager.Get("foo")

	assert.Equal(t, uint32(0), shardId)
}

func TestOverlap(t *testing.T) {
	for _, item := range []struct {
		a         HashRange
		b         HashRange
		isOverlap bool
	}{
		{hashRange(1, 2), hashRange(3, 6), false},
		{hashRange(1, 4), hashRange(3, 6), true},
		{hashRange(4, 5), hashRange(3, 6), true},
		{hashRange(5, 8), hashRange(3, 6), true},
		{hashRange(7, 8), hashRange(3, 6), false},
	} {
		assert.Equal(t, overlap(item.a, item.b), item.isOverlap)
	}
}
