package oxia

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/server/kv"
	"oxia/server/wal"
	"oxia/standalone"
	"testing"
	"time"
)

var (
	versionZero int64 = 0
)

func init() {
	common.ConfigureLogger()
}

func TestAsyncClientImpl(t *testing.T) {
	kvOptions := kv.KVFactoryOptions{InMemory: true}
	kvFactory, err := kv.NewPebbleKVFactory(&kvOptions)
	assert.NoError(t, err)
	defer kvFactory.Close()
	walFactory := wal.NewInMemoryWalFactory()
	defer walFactory.Close()
	server, err := standalone.NewStandaloneRpcServer(standalone.Config{}, "localhost:0", "localhost", 1, walFactory, kvFactory)
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.Port())
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

func TestAsyncClientImpl_Notifications(t *testing.T) {
	kvOptions := kv.KVFactoryOptions{InMemory: true}
	kvFactory, _ := kv.NewPebbleKVFactory(&kvOptions)
	defer kvFactory.Close()
	walFactory := wal.NewInMemoryWalFactory()
	defer walFactory.Close()

	server, err := standalone.NewStandaloneRpcServer(standalone.Config{}, "localhost:0", "localhost", 3, walFactory, kvFactory)
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", server.Port())
	client, err := NewSyncClient(serviceAddress, WithBatchLinger(0))
	assert.NoError(t, err)

	notificationsCh := client.GetNotifications()

	s1, _ := client.Put("/a", []byte("0"), nil)

	n := <-notificationsCh
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s1.Version, n.Version)

	s2, _ := client.Put("/a", []byte("1"), nil)

	n = <-notificationsCh
	assert.Equal(t, KeyModified, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s2.Version, n.Version)

	s3, _ := client.Put("/b", []byte("0"), nil)
	assert.NoError(t, client.Delete("/a", nil))

	n = <-notificationsCh
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/b", n.Key)
	assert.Equal(t, s3.Version, n.Version)

	n = <-notificationsCh
	assert.Equal(t, KeyDeleted, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.EqualValues(t, -1, n.Version)

	// Create a 2nd notifications channel
	// This will only receive new updates
	notificationsCh2 := client.GetNotifications()

	select {
	case <-notificationsCh2:
		assert.Fail(t, "shouldn't have received any notifications")
	case <-time.After(1 * time.Second):
		// Ok, we expect it to time out
	}

	s4, _ := client.Put("/x", []byte("1"), nil)

	n = <-notificationsCh
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.Version, n.Version)

	n = <-notificationsCh2
	assert.Equal(t, KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.Version, n.Version)

	////

	assert.NoError(t, client.Close())

	// Channels should be closed after the client is closed
	select {
	case <-notificationsCh:
		// Ok

	case <-time.After(3600 * time.Second):
		assert.Fail(t, "should have been closed")
	}

	select {
	case <-notificationsCh2:
		// Ok

	case <-time.After(3600 * time.Second):
		assert.Fail(t, "should have been closed")
	}

	assert.NoError(t, server.Close())
}
