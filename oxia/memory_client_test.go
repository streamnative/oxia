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
	versionId *int64
	version   Version
	err       error
}

type deleteItem struct {
	versionId *int64
	err       error
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

func runTest(test func(AsyncClient)) {
	client := newMemoryClientWithClock(&clockStub{
		millis: []uint64{1, 2, 3},
	})
	test(client)
}

func runTests[ITEM any](items []ITEM, test func(AsyncClient, ITEM)) {
	for _, item := range items {
		runTest(func(client AsyncClient) {
			test(client, item)
		})
	}
}

func ptr(t int64) *int64 {
	return &t
}

func put(t *testing.T, client AsyncClient, key string) {
	c := client.Put(key, payload0, nil)
	response := <-c
	assert.ErrorIs(t, nil, response.Err)
}

func TestClose(t *testing.T) {
	runTest(func(client AsyncClient) {
		err := client.Close()

		assert.ErrorIs(t, nil, err)
	})
}

func TestPutNew(t *testing.T) {
	items := []putItem{
		{nil, Version{
			VersionId:         1,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 1,
		}, nil},
		{ptr(VersionNotExists), Version{
			VersionId:         1,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 1,
		}, nil},
		{ptr(1), Version{}, ErrorUnexpectedVersion},
	}
	runTests(items, func(client AsyncClient, item putItem) {
		c := client.Put(key, payload1, item.versionId)
		response := <-c

		assert.Equal(t, item.version, response.Version)
		assert.ErrorIs(t, item.err, response.Err)
	})
}

func TestPutExisting(t *testing.T) {
	items := []putItem{
		{nil, Version{
			VersionId:         2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
		{ptr(VersionNotExists), Version{}, ErrorUnexpectedVersion},
		{ptr(1), Version{
			VersionId:         2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
	}
	runTests(items, func(client AsyncClient, item putItem) {
		put(t, client, key)

		c := client.Put(key, payload1, item.versionId)
		response := <-c

		assert.Equal(t, item.version, response.Version)
		assert.ErrorIs(t, item.err, response.Err)
	})
}

func TestDeleteMissing(t *testing.T) {
	items := []deleteItem{
		{nil, ErrorKeyNotFound},
		{ptr(VersionNotExists), ErrorKeyNotFound},
		{ptr(1), ErrorKeyNotFound},
	}
	runTests(items, func(client AsyncClient, item deleteItem) {
		c := client.Delete(key, item.versionId)
		err := <-c

		assert.ErrorIs(t, item.err, err)
	})
}

func TestDeleteExisting(t *testing.T) {
	items := []deleteItem{
		{nil, nil},
		{ptr(VersionNotExists), ErrorUnexpectedVersion},
		{ptr(1), nil},
	}
	runTests(items, func(client AsyncClient, item deleteItem) {
		put(t, client, key)

		c := client.Delete(key, item.versionId)
		err := <-c

		assert.ErrorIs(t, item.err, err)
	})
}

func TestDeleteRange(t *testing.T) {
	runTest(func(client AsyncClient) {
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
	runTest(func(client AsyncClient) {
		c := client.Get(key)
		response := <-c

		assert.Equal(t, GetResult{
			Err: ErrorKeyNotFound,
		}, response)
	})
}

func TestGetExisting(t *testing.T) {
	runTest(func(client AsyncClient) {
		put(t, client, key)

		c := client.Get(key)
		response := <-c

		assert.Equal(t, GetResult{
			Payload: payload0,
			Version: Version{
				VersionId:         1,
				CreatedTimestamp:  1,
				ModifiedTimestamp: 1,
			},
		}, response)
		assert.ErrorIs(t, nil, response.Err)
	})
}

func TestGetRange(t *testing.T) {
	runTest(func(client AsyncClient) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		c := client.GetRange("/b", "/c")
		response := <-c

		assert.Equal(t, []string{"/b"}, response.Keys)
		assert.ErrorIs(t, nil, response.Err)
	})
}
