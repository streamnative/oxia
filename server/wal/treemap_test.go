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

package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreeMapPut(t *testing.T) {
	tm := newInt64TreeMap[int]()
	tm.Put(1, 2)

	v, ok := tm.tree.Get(int64(1))
	assert.True(t, ok)
	assert.Equal(t, int(2), v)
}

func TestTreeMapGet(t *testing.T) {
	tm := newInt64TreeMap[int]()
	tm.Put(1, 2)

	v, ok := tm.Get(int64(1))
	assert.True(t, ok)
	assert.Equal(t, int(2), v)

	v, ok = tm.Get(3)
	assert.False(t, ok)
	assert.Equal(t, int(0), v)
}

func TestTreeMapRemove(t *testing.T) {
	tm := newInt64TreeMap[int]()
	tm.Put(1, 2)

	v, ok := tm.Get(int64(1))
	assert.True(t, ok)
	assert.Equal(t, int(2), v)

	tm.Remove(3)

	v, ok = tm.Get(int64(1))
	assert.True(t, ok)
	assert.Equal(t, int(2), v)

	tm.Remove(1)

	v, ok = tm.Get(int64(1))
	assert.False(t, ok)
	assert.Equal(t, int(0), v)
}

func TestTreeMapEmpty(t *testing.T) {
	tm := newInt64TreeMap[int]()
	assert.True(t, tm.Empty())

	tm.Put(1, 2)
	assert.False(t, tm.Empty())
}

func TestTreeMapSize(t *testing.T) {
	tm := newInt64TreeMap[int]()
	assert.Equal(t, 0, tm.Size())

	tm.Put(1, 2)
	assert.Equal(t, 1, tm.Size())
}

func TestTreeMapKeys(t *testing.T) {
	tm := newInt64TreeMap[int]()
	assert.Equal(t, []int64{}, tm.Keys())

	tm.Put(1, 2)
	tm.Put(3, 4)
	assert.Equal(t, []int64{1, 3}, tm.Keys())
}

func TestTreeMapClear(t *testing.T) {
	tm := newInt64TreeMap[int]()

	tm.Put(1, 2)
	tm.Put(3, 4)
	assert.Equal(t, 2, tm.Size())

	tm.Clear()
	assert.Equal(t, 0, tm.Size())
}

func TestTreeMapMin(t *testing.T) {
	tm := newInt64TreeMap[int]()

	k, v := tm.Min()
	assert.Equal(t, int64(0), k)
	assert.Equal(t, int(0), v)

	tm.Put(1, 2)
	tm.Put(3, 4)

	k, v = tm.Min()
	assert.Equal(t, int64(1), k)
	assert.Equal(t, int(2), v)
}

func TestTreeMapMax(t *testing.T) {
	tm := newInt64TreeMap[int]()

	k, v := tm.Max()
	assert.Equal(t, int64(0), k)
	assert.Equal(t, int(0), v)

	tm.Put(1, 2)
	tm.Put(3, 4)

	k, v = tm.Max()
	assert.Equal(t, int64(3), k)
	assert.Equal(t, int(4), v)
}

func TestTreeMapFloor(t *testing.T) {
	tm := newInt64TreeMap[int]()

	tm.Put(1, 2)
	tm.Put(5, 6)

	k, v := tm.Floor(3)
	assert.Equal(t, int64(1), k)
	assert.Equal(t, int(2), v)

	k, v = tm.Floor(5)
	assert.Equal(t, int64(5), k)
	assert.Equal(t, int(6), v)

	k, v = tm.Floor(0)
	assert.Equal(t, int64(0), k)
	assert.Equal(t, int(0), v)
}

func TestTreeMapString(t *testing.T) {
	tm := newInt64TreeMap[int]()

	tm.Put(1, 2)
	tm.Put(5, 6)

	assert.Regexp(t, "RedBlackTree", tm.String())
}

func TestTreeMapEach(t *testing.T) {
	tm := newInt64TreeMap[int]()

	tm.Put(1, 2)
	tm.Put(5, 6)
	tm.Put(7, 8)

	var keys []int64
	tm.Each(func(k int64, v int) bool {
		keys = append(keys, k)
		return true
	})
	assert.Equal(t, []int64{1, 5, 7}, keys)

	keys = []int64{}
	count := 0
	tm.Each(func(k int64, v int) bool {
		count++
		if k > 1 {
			return false
		}

		keys = append(keys, k)
		return true
	})
	assert.Equal(t, []int64{1}, keys)
	assert.Equal(t, 2, count)
}

func TestTreeMapToKey(t *testing.T) {
	tm := newInt64TreeMap[int]()
	assert.Equal(t, int64(1), tm.toKey(int64(1)))

	assert.PanicsWithError(t, "expect key int64, got string from treemap", func() {
		tm.toKey("1")
	})
}

func TestTreeMapToValue(t *testing.T) {
	tm := newInt64TreeMap[int]()
	assert.Equal(t, int(1), tm.toValue(int(1)))

	assert.PanicsWithError(t, "expect value int, got string from treemap", func() {
		tm.toValue("1")
	})
}
