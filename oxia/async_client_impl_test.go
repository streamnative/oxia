package oxia

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"oxia/server/kv"
	"oxia/server/wal"
	"oxia/standalone"
	"testing"
)

var (
	versionZero int64 = 0
)

func TestAsyncClientImpl(t *testing.T) {
	kvOptions := kv.KVFactoryOptions{InMemory: true}
	kvFactory, err := kv.NewPebbleKVFactory(&kvOptions)
	assert.NoError(t, err)
	defer kvFactory.Close()
	walFactory := wal.NewInMemoryWalFactory()
	defer walFactory.Close()
	server, err := standalone.NewStandaloneRpcServer("localhost:0", "localhost", 1, walFactory, kvFactory)
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.Container.Port())
	client, err := NewAsyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)

	putResult := <-client.Put("/a", []byte{0}, &VersionNotExists)
	assert.Equal(t, versionZero, putResult.Stat.Version)

	getResult := <-client.Get("/a")
	assert.Equal(t, GetResult{
		Payload: []byte{0},
		Stat:    putResult.Stat,
	}, getResult)

	putResult = <-client.Put("/c", []byte{0}, &VersionNotExists)
	assert.Equal(t, versionZero, putResult.Stat.Version)

	putResult = <-client.Put("/c", []byte{1}, &versionZero)
	assert.Equal(t, int64(1), putResult.Stat.Version)

	getRangeResult := <-client.List("/a", "/d")
	assert.Equal(t, ListResult{
		Keys: []string{"/a", "/c"},
	}, getRangeResult)

	deleteErr := <-client.Delete("/a", &versionZero)
	assert.NoError(t, deleteErr)

	getResult = <-client.Get("/a")
	assert.Equal(t, GetResult{
		Err: ErrorKeyNotFound,
	}, getResult)

	deleteRangeResult := <-client.DeleteRange("/c", "/d")
	assert.NoError(t, deleteRangeResult)

	getResult = <-client.Get("/d")
	assert.Equal(t, GetResult{
		Err: ErrorKeyNotFound,
	}, getResult)

	err = client.Close()
	assert.NoError(t, err)

	err = server.Close()
	assert.NoError(t, err)
}
