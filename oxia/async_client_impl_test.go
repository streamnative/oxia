package oxia

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"oxia/server/kv"
	"oxia/standalone"
	"testing"
)

var (
	versionZero int64 = 0
)

func TestAsyncClientImpl(t *testing.T) {
	kvOptions := kv.KVFactoryOptions{InMemory: true}
	kvFactory := kv.NewPebbleKVFactory(&kvOptions)
	server, err := standalone.NewStandaloneRpcServer(0, "localhost", 1, kvFactory)
	assert.ErrorIs(t, nil, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.Container.Port())
	options, err := NewClientOptions(serviceAddress, WithBatchLinger(0))
	if err != nil {
		assert.Fail(t, err.Error())
	}
	client := NewAsyncClient(options)

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

	getRangeResult := <-client.GetRange("/a", "/d")
	assert.Equal(t, GetRangeResult{
		Keys: []string{"/a", "/c"},
	}, getRangeResult)

	deleteErr := <-client.Delete("/a", &versionZero)
	assert.ErrorIs(t, nil, deleteErr)

	getResult = <-client.Get("/a")
	assert.Equal(t, GetResult{
		Err: ErrorKeyNotFound,
	}, getResult)

	deleteRangeResult := <-client.DeleteRange("/c", "/d")
	assert.ErrorIs(t, nil, deleteRangeResult)

	getResult = <-client.Get("/d")
	assert.Equal(t, GetResult{
		Err: ErrorKeyNotFound,
	}, getResult)

	err = client.Close()
	assert.ErrorIs(t, nil, err)

	err = server.Close()
	assert.ErrorIs(t, nil, err)
}
