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
	"fmt"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
)

type treeMap[K comparable, V any] struct {
	tree *rbt.Tree
}

func newInt64TreeMap[V any]() *treeMap[int64, V] {
	return &treeMap[int64, V]{
		tree: rbt.NewWith(utils.Int64Comparator),
	}
}

func (m *treeMap[K, V]) Put(key K, value V) {
	m.tree.Put(key, value)
}

func (m *treeMap[K, V]) Get(key K) (value V, found bool) {
	v, ok := m.tree.Get(key)
	if !ok {
		return *new(V), false
	}

	return m.toValue(v), true
}

func (m *treeMap[K, V]) Remove(key K) {
	m.tree.Remove(key)
}

func (m *treeMap[K, V]) Empty() bool {
	return m.tree.Empty()
}

func (m *treeMap[K, V]) Size() int {
	return m.tree.Size()
}

func (m *treeMap[K, V]) Keys() []K {
	keys := make([]K, 0, m.tree.Size())
	m.Each(func(k K, v V) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

func (m *treeMap[K, V]) Clear() {
	m.tree.Clear()
}

func (m *treeMap[K, V]) Min() (K, V) {
	if node := m.tree.Left(); node != nil {
		return m.toKey(node.Key), m.toValue(node.Value)
	}

	return *new(K), *new(V)
}

func (m *treeMap[K, V]) Max() (K, V) {
	if node := m.tree.Right(); node != nil {
		return m.toKey(node.Key), m.toValue(node.Value)
	}

	return *new(K), *new(V)
}

func (m *treeMap[K, V]) Floor(key K) (K, V) {
	node, found := m.tree.Floor(key)
	if !found {
		return *new(K), *new(V)
	}

	return m.toKey(node.Key), m.toValue(node.Value)
}

func (m *treeMap[K, V]) String() string {
	return m.tree.String()
}

func (m *treeMap[K, V]) Each(f func(K, V) bool) {
	iterator := m.tree.Iterator()
	for iterator.Next() {
		if !f(m.toKey(iterator.Key()), m.toValue(iterator.Value())) {
			return
		}
	}
}

func (*treeMap[K, V]) toKey(key any) K {
	kk, ok := key.(K)
	if !ok {
		panic(fmt.Errorf("expect key %T, got %T from treemap", *new(K), key))
	}

	return kk
}

func (*treeMap[K, V]) toValue(value any) V {
	vv, ok := value.(V)
	if !ok {
		panic(fmt.Errorf("expect value %T, got %T from treemap", *new(V), value))
	}

	return vv
}
