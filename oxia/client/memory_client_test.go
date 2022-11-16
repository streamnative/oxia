package client

import (
	"github.com/stretchr/testify/assert"
	"oxia/oxia"
	"sort"
	"testing"
)

var (
	key      = "/a"
	payload0 = []byte{0}
	payload1 = []byte{1}
)

type putItem struct {
	version *int64
	stat    oxia.Stat
	err     error
}

type deleteItem struct {
	version *int64
	err     error
}

type clockStub struct {
	millis []uint64
	index  int
}

func (c *clockStub) NowMillis() uint64 {
	millis := c.millis[c.index]
	c.index++
	return millis
}

func runTest(test func(oxia.Client)) {
	client := newMemoryClientWithClock(&clockStub{
		millis: []uint64{1, 2, 3},
	})
	test(client)
}

func runTests[ITEM any](items []ITEM, test func(oxia.Client, ITEM)) {
	for _, item := range items {
		runTest(func(client oxia.Client) {
			test(client, item)
		})
	}
}

func ptr(t int64) *int64 {
	return &t
}

func put(t *testing.T, client oxia.Client, key string) {
	c := client.Put(key, payload0, nil)
	response := <-c
	assert.ErrorIs(t, nil, response.Err)
}

func TestClose(t *testing.T) {
	runTest(func(client oxia.Client) {
		err := client.Close()

		assert.ErrorIs(t, nil, err)
	})
}

func TestPutNew(t *testing.T) {
	items := []putItem{
		{nil, oxia.Stat{
			Version:           1,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 1,
		}, nil},
		{ptr(oxia.VersionNotExists), oxia.Stat{
			Version:           1,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 1,
		}, nil},
		{ptr(1), oxia.Stat{}, oxia.ErrorBadVersion},
	}
	runTests(items, func(client oxia.Client, item putItem) {
		c := client.Put(key, payload1, item.version)
		response := <-c

		assert.Equal(t, item.stat, response.Stat)
		assert.ErrorIs(t, item.err, response.Err)
	})
}

func TestPutExisting(t *testing.T) {
	items := []putItem{
		{nil, oxia.Stat{
			Version:           2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
		{ptr(oxia.VersionNotExists), oxia.Stat{}, oxia.ErrorBadVersion},
		{ptr(1), oxia.Stat{
			Version:           2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
	}
	runTests(items, func(client oxia.Client, item putItem) {
		put(t, client, key)

		c := client.Put(key, payload1, item.version)
		response := <-c

		assert.Equal(t, item.stat, response.Stat)
		assert.ErrorIs(t, item.err, response.Err)
	})
}

func TestDeleteMissing(t *testing.T) {
	items := []deleteItem{
		{nil, oxia.ErrorKeyNotFound},
		{ptr(oxia.VersionNotExists), oxia.ErrorKeyNotFound},
		{ptr(1), oxia.ErrorKeyNotFound},
	}
	runTests(items, func(client oxia.Client, item deleteItem) {
		c := client.Delete(key, item.version)
		err := <-c

		assert.ErrorIs(t, item.err, err)
	})
}

func TestDeleteExisting(t *testing.T) {
	items := []deleteItem{
		{nil, nil},
		{ptr(oxia.VersionNotExists), oxia.ErrorBadVersion},
		{ptr(1), nil},
	}
	runTests(items, func(client oxia.Client, item deleteItem) {
		put(t, client, key)

		c := client.Delete(key, item.version)
		err := <-c

		assert.ErrorIs(t, item.err, err)
	})
}

func TestDeleteRange(t *testing.T) {
	runTest(func(client oxia.Client) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		c1 := client.DeleteRange("/b", "/c")
		err := <-c1
		assert.ErrorIs(t, err, nil)

		c2 := client.GetRange("/a", "/d")
		response := <-c2

		sort.Strings(response.Keys)
		assert.Equal(t, []string{"/a", "/c"}, response.Keys)
		assert.ErrorIs(t, nil, response.Err)
	})
}

func TestGetMissing(t *testing.T) {
	runTest(func(client oxia.Client) {
		c := client.Get(key)
		response := <-c

		assert.Equal(t, oxia.Value{}, response.Value)
		assert.ErrorIs(t, oxia.ErrorKeyNotFound, response.Err)
	})
}

func TestGetExisting(t *testing.T) {
	runTest(func(client oxia.Client) {
		put(t, client, key)

		c := client.Get(key)
		response := <-c

		assert.Equal(t, oxia.Value{
			Payload: payload0,
			Stat: oxia.Stat{
				Version:           1,
				CreatedTimestamp:  1,
				ModifiedTimestamp: 1,
			},
		}, response.Value)
		assert.ErrorIs(t, nil, response.Err)
	})
}

func TestGetRange(t *testing.T) {
	runTest(func(client oxia.Client) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		c := client.GetRange("/b", "/c")
		response := <-c

		assert.Equal(t, []string{"/b"}, response.Keys)
		assert.ErrorIs(t, nil, response.Err)
	})
}
