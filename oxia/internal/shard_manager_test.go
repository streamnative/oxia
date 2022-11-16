package internal

import (
	"github.com/stretchr/testify/assert"
	"net"
	"oxia/common"
	"oxia/oxia"
	"oxia/server/kv"
	"oxia/standalone"
	"strconv"
	"testing"
)

type testShardStrategy struct {
}

func (s *testShardStrategy) Get(key string) func(oxia.Shard) bool {
	return func(shard oxia.Shard) bool {
		return true
	}
}

func TestWithStandalone(t *testing.T) {
	port := getFreePort()

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
		a         oxia.HashRange
		b         oxia.HashRange
		isOverlap bool
	}{
		{oxia.HashRange{1, 2}, oxia.HashRange{3, 6}, false},
		{oxia.HashRange{1, 4}, oxia.HashRange{3, 6}, true},
		{oxia.HashRange{4, 5}, oxia.HashRange{3, 6}, true},
		{oxia.HashRange{5, 8}, oxia.HashRange{3, 6}, true},
		{oxia.HashRange{7, 8}, oxia.HashRange{3, 6}, false},
	} {
		assert.Equal(t, overlap(item.a, item.b), item.isOverlap)
	}
}

func getFreePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic("could not find free port")
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic("could not find free port")
	}
	defer func() {
		err := l.Close()
		if err != nil {
			panic("could not find free port")
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}
