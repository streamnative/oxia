package impl

import (
	"github.com/stretchr/testify/assert"
	"oxia/internal/client"
	"oxia/oxia"
	"oxia/server/kv"
	"oxia/standalone"
	"strconv"
	"testing"
)

var (
	versionZero int64 = 0
)

func TestClientImpl(t *testing.T) {
	port := client.GetFreePort()

	kvOptions := kv.KVFactoryOptions{InMemory: true}
	kvFactory := kv.NewPebbleKVFactory(&kvOptions)
	server, err := standalone.NewStandaloneRpcServer(port, "localhost", 1, kvFactory)
	assert.ErrorIs(t, nil, err)

	options := oxia.Options{
		ServiceUrl:   "localhost:" + strconv.FormatInt(int64(port), 10),
		BatchLinger:  oxia.DefaultBatchLinger,
		BatchMaxSize: 1,
		BatchTimeout: oxia.DefaultBatchTimeout,
	}
	oxiaClient := NewClient(&options)

	putResult := <-oxiaClient.Put("/a", []byte{0}, &oxia.VersionNotExists)
	assert.Equal(t, versionZero, putResult.Stat.Version)

	getResult := <-oxiaClient.Get("/a")
	assert.Equal(t, oxia.GetResult{
		Payload: []byte{0},
		Stat:    putResult.Stat,
	}, getResult)

	putResult = <-oxiaClient.Put("/c", []byte{0}, &oxia.VersionNotExists)
	assert.Equal(t, versionZero, putResult.Stat.Version)

	putResult = <-oxiaClient.Put("/c", []byte{1}, &versionZero)
	assert.Equal(t, int64(1), putResult.Stat.Version)

	getRangeResult := <-oxiaClient.GetRange("/a", "/d")
	assert.Equal(t, oxia.GetRangeResult{
		Keys: []string{"/a", "/c"},
	}, getRangeResult)

	deleteErr := <-oxiaClient.Delete("/a", &versionZero)
	assert.ErrorIs(t, nil, deleteErr)

	getResult = <-oxiaClient.Get("/a")
	assert.Equal(t, oxia.GetResult{
		Err: oxia.ErrorKeyNotFound,
	}, getResult)

	deleteRangeResult := <-oxiaClient.DeleteRange("/c", "/d")
	assert.ErrorIs(t, nil, deleteRangeResult)

	getResult = <-oxiaClient.Get("/d")
	assert.Equal(t, oxia.GetResult{
		Err: oxia.ErrorKeyNotFound,
	}, getResult)

	err = oxiaClient.Close()
	assert.ErrorIs(t, nil, err)

	err = server.Close()
	assert.ErrorIs(t, nil, err)
}
