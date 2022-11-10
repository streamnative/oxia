package oxia

import (
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/server/kv"
	"oxia/standalone"
	"testing"
)

// TODO don't use fixed port
func TestWithStandalone(t *testing.T) {
	kvOptions := kv.KVFactoryOptions{InMemory: true}
	kvFactory := kv.NewPebbleKVFactory(&kvOptions)
	_, err := standalone.NewStandaloneRpcServer(5647, "localhost", 2, kvFactory)
	assert.ErrorIs(t, nil, err)

	clientPool := common.NewClientPool()
	serviceUrl := "localhost:5647"
	shardManager := newShardManager(NewHashRangeShardStrategy(common.Xxh332), clientPool, serviceUrl)

	<-shardManager.start()

	shardId, err := shardManager.get("foo")

	assert.Equal(t, uint32(0), shardId)
	assert.ErrorIs(t, nil, err)
}

func TestOverlap(t *testing.T) {
	for _, item := range []struct {
		a         HashRange
		b         HashRange
		isOverlap bool
	}{
		{HashRange{1, 2}, HashRange{3, 6}, false},
		{HashRange{1, 4}, HashRange{3, 6}, true},
		{HashRange{4, 5}, HashRange{3, 6}, true},
		{HashRange{5, 8}, HashRange{3, 6}, true},
		{HashRange{7, 8}, HashRange{3, 6}, false},
	} {
		assert.Equal(t, overlap(&item.a, &item.b), item.isOverlap)
	}
}
