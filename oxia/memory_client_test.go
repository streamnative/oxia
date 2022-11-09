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
	_, err := client.Put(key, payload0, nil)
	assert.ErrorIs(t, err, nil)
}

func TestClose(t *testing.T) {
	runTest(func(client Client) {
		err := client.Close()
		assert.ErrorIs(t, err, nil)
	})
}

func TestPutNew(t *testing.T) {
	items := []putItem{
		{nil, Stat{1, 1, 1}, nil},
		{ptr(VersionNotExists), Stat{1, 1, 1}, nil},
		{ptr(1), Stat{}, ErrorBadVersion},
	}
	runTests(items, func(client Client, item putItem) {

		stat, err := client.Put(key, payload1, item.version)

		assert.Equal(t, stat, item.stat)
		assert.ErrorIs(t, err, item.err)
	})
}

func TestPutExisting(t *testing.T) {
	items := []putItem{
		{nil, Stat{2, 1, 2}, nil},
		{ptr(VersionNotExists), Stat{}, ErrorBadVersion},
		{ptr(1), Stat{2, 1, 2}, nil},
	}
	runTests(items, func(client Client, item putItem) {
		put(t, client, key)

		stat, err := client.Put(key, payload1, item.version)

		assert.Equal(t, stat, item.stat)
		assert.ErrorIs(t, err, item.err)
	})
}

func TestDeleteMissing(t *testing.T) {
	items := []deleteItem{
		{nil, ErrorKeyNotFound},
		{ptr(VersionNotExists), ErrorKeyNotFound},
		{ptr(1), ErrorKeyNotFound},
	}
	runTests(items, func(client Client, item deleteItem) {
		err := client.Delete(key, item.version)

		assert.ErrorIs(t, err, item.err)
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

		err := client.Delete(key, item.version)

		assert.ErrorIs(t, err, item.err)
	})
}

func TestDeleteRange(t *testing.T) {
	runTest(func(client Client) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		err := client.DeleteRange("/b", "/c")
		assert.ErrorIs(t, err, nil)

		keys, err := client.GetRange("/a", "/d")

		sort.Strings(keys)
		assert.Equal(t, keys, []string{"/a", "/c"})
		assert.ErrorIs(t, err, nil)
	})
}

func TestGetMissing(t *testing.T) {
	runTest(func(client Client) {
		value, err := client.Get(key)

		assert.Equal(t, value, Value{})
		assert.ErrorIs(t, err, ErrorKeyNotFound)
	})
}

func TestGetExisting(t *testing.T) {
	runTest(func(client Client) {
		put(t, client, key)

		value, err := client.Get(key)

		assert.Equal(t, value, Value{payload0, Stat{1, 1, 1}})
		assert.ErrorIs(t, err, nil)
	})
}

func TestGetRange(t *testing.T) {
	runTest(func(client Client) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		keys, err := client.GetRange("/b", "/c")

		assert.Equal(t, keys, []string{"/b"})
		assert.ErrorIs(t, err, nil)
	})
}
