package oxia

import (
	"github.com/stretchr/testify/assert"
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
	stat    Stat
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

func runTest(test func(Client)) {
	client := newMemoryClientWithClock(&clockStub{
		millis: []uint64{1, 2, 3},
	})
	test(client)
}

func runTests[ITEM any](items []ITEM, test func(Client, ITEM)) {
	for _, item := range items {
		runTest(func(client Client) {
			test(client, item)
		})
	}
}

func ptr(t int64) *int64 {
	return &t
}

func put(t *testing.T, client Client, key string) {
	c := client.Put(key, payload0, nil)
	response := <-c
	assert.ErrorIs(t, nil, response.Err)
}

func TestClose(t *testing.T) {
	runTest(func(client Client) {
		err := client.Close()

		assert.ErrorIs(t, nil, err)
	})
}

func TestPutNew(t *testing.T) {
	items := []putItem{
		{nil, Stat{
			Version:           1,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 1,
		}, nil},
		{ptr(VersionNotExists), Stat{
			Version:           1,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 1,
		}, nil},
		{ptr(1), Stat{}, ErrorBadVersion},
	}
	runTests(items, func(client Client, item putItem) {
		c := client.Put(key, payload1, item.version)
		response := <-c

		assert.Equal(t, item.stat, response.Stat)
		assert.ErrorIs(t, item.err, response.Err)
	})
}

func TestPutExisting(t *testing.T) {
	items := []putItem{
		{nil, Stat{
			Version:           2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
		{ptr(VersionNotExists), Stat{}, ErrorBadVersion},
		{ptr(1), Stat{
			Version:           2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
	}
	runTests(items, func(client Client, item putItem) {
		put(t, client, key)

		c := client.Put(key, payload1, item.version)
		response := <-c

		assert.Equal(t, item.stat, response.Stat)
		assert.ErrorIs(t, item.err, response.Err)
	})
}

func TestDeleteMissing(t *testing.T) {
	items := []deleteItem{
		{nil, ErrorKeyNotFound},
		{ptr(VersionNotExists), ErrorKeyNotFound},
		{ptr(1), ErrorKeyNotFound},
	}
	runTests(items, func(client Client, item deleteItem) {
		c := client.Delete(key, item.version)
		err := <-c

		assert.ErrorIs(t, item.err, err)
	})
}

func TestDeleteExisting(t *testing.T) {
	items := []deleteItem{
		{nil, nil},
		{ptr(VersionNotExists), ErrorBadVersion},
		{ptr(1), nil},
	}
	runTests(items, func(client Client, item deleteItem) {
		put(t, client, key)

		c := client.Delete(key, item.version)
		err := <-c

		assert.ErrorIs(t, item.err, err)
	})
}

func TestDeleteRange(t *testing.T) {
	runTest(func(client Client) {
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
	runTest(func(client Client) {
		c := client.Get(key)
		response := <-c

		assert.Equal(t, Value{}, response.Value)
		assert.ErrorIs(t, ErrorKeyNotFound, response.Err)
	})
}

func TestGetExisting(t *testing.T) {
	runTest(func(client Client) {
		put(t, client, key)

		c := client.Get(key)
		response := <-c

		assert.Equal(t, Value{
			Payload: payload0,
			Stat: Stat{
				Version:           1,
				CreatedTimestamp:  1,
				ModifiedTimestamp: 1,
			},
		}, response.Value)
		assert.ErrorIs(t, nil, response.Err)
	})
}

func TestGetRange(t *testing.T) {
	runTest(func(client Client) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		c := client.GetRange("/b", "/c")
		response := <-c

		assert.Equal(t, []string{"/b"}, response.Keys)
		assert.ErrorIs(t, nil, response.Err)
	})
}
