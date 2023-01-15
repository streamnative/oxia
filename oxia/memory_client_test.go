// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oxia

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"
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

func (c *clockStub) Now() time.Time {
	millis := c.millis[c.index]
	c.index++
	return time.UnixMilli(int64(millis))
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
	assert.NoError(t, response.Err)
}

func TestClose(t *testing.T) {
	runTest(func(client AsyncClient) {
		err := client.Close()

		assert.NoError(t, err)
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
		{ptr(1), Stat{}, ErrorUnexpectedVersion},
	}
	runTests(items, func(client AsyncClient, item putItem) {
		c := client.Put(key, payload1, item.version)
		response := <-c

		assert.Equal(t, item.stat, response.Stat)
		assert.ErrorIs(t, response.Err, item.err)
	})
}

func TestPutExisting(t *testing.T) {
	items := []putItem{
		{nil, Stat{
			Version:           2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
		{ptr(VersionNotExists), Stat{}, ErrorUnexpectedVersion},
		{ptr(1), Stat{
			Version:           2,
			CreatedTimestamp:  1,
			ModifiedTimestamp: 2,
		}, nil},
	}
	runTests(items, func(client AsyncClient, item putItem) {
		put(t, client, key)

		c := client.Put(key, payload1, item.version)
		response := <-c

		assert.Equal(t, item.stat, response.Stat)
		assert.ErrorIs(t, response.Err, item.err)
	})
}

func TestDeleteMissing(t *testing.T) {
	items := []deleteItem{
		{nil, ErrorKeyNotFound},
		{ptr(VersionNotExists), ErrorKeyNotFound},
		{ptr(1), ErrorKeyNotFound},
	}
	runTests(items, func(client AsyncClient, item deleteItem) {
		c := client.Delete(key, item.version)
		err := <-c

		assert.ErrorIs(t, err, item.err)
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

		c := client.Delete(key, item.version)
		err := <-c

		assert.ErrorIs(t, err, item.err)
	})
}

func TestDeleteRange(t *testing.T) {
	runTest(func(client AsyncClient) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		c1 := client.DeleteRange("/b", "/c")
		err := <-c1
		assert.NoError(t, err)

		c2 := client.List("/a", "/d")
		response := <-c2

		sort.Strings(response.Keys)
		assert.Equal(t, []string{"/a", "/c"}, response.Keys)
		assert.NoError(t, response.Err)
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
			Stat: Stat{
				Version:           1,
				CreatedTimestamp:  1,
				ModifiedTimestamp: 1,
			},
		}, response)
		assert.NoError(t, response.Err)
	})
}

func TestList(t *testing.T) {
	runTest(func(client AsyncClient) {
		put(t, client, "/a")
		put(t, client, "/b")
		put(t, client, "/c")

		c := client.List("/b", "/c")
		response := <-c

		assert.Equal(t, []string{"/b"}, response.Keys)
		assert.NoError(t, response.Err)
	})
}
