// Copyright 2023-2026 The Oxia Authors
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

package raft

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func newTestStore(t *testing.T) *kvRaftStore {
	t.Helper()
	store, err := newKVRaftStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	return store
}

func storeLogs(t *testing.T, store *kvRaftStore, indexes ...uint64) {
	t.Helper()
	logs := make([]*hashicorpraft.Log, len(indexes))
	for i, index := range indexes {
		logs[i] = &hashicorpraft.Log{Index: index, Term: 1, Data: []byte("entry")}
	}
	require.NoError(t, store.StoreLogs(logs))
}

func TestKVRaftStoreLogRoundTrip(t *testing.T) {
	store := newTestStore(t)

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 0, first)

	storeLogs(t, store, 1, 2, 3, 4, 5)

	first, err = store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 1, first)

	last, err := store.LastIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 5, last)

	log := &hashicorpraft.Log{}
	require.NoError(t, store.GetLog(3, log))
	assert.EqualValues(t, 3, log.Index)
	assert.Equal(t, []byte("entry"), log.Data)
}

func TestNewKVRaftStoreFailsOnFilePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	_, err := newKVRaftStore(path)
	require.Error(t, err)
}

func TestNewKVRaftStoreFailsWhenStoreIsOpen(t *testing.T) {
	path := t.TempDir()
	store, err := newKVRaftStore(path)
	require.NoError(t, err)

	_, err = newKVRaftStore(path)
	assert.ErrorContains(t, err, "lock")

	storeLogs(t, store, 1)
	assert.NoError(t, store.Close())
}

func TestKVRaftStoreGetLogMissing(t *testing.T) {
	store := newTestStore(t)
	storeLogs(t, store, 1)

	require.Error(t, store.GetLog(2, &hashicorpraft.Log{}))
}

func TestKVRaftStoreReadsReturnKVError(t *testing.T) {
	errKV := errors.New("kv failure")
	store := &kvRaftStore{kv: &fakeKV{
		get: func(string) (string, []byte, io.Closer, error) { return "", nil, nil, errKV },
	}}

	_, err := store.FirstIndex()
	require.ErrorIs(t, err, errKV)
	_, err = store.LastIndex()
	require.ErrorIs(t, err, errKV)
	_, err = store.Get([]byte("key"))
	require.ErrorIs(t, err, errKV)
	_, err = store.GetUint64([]byte("key"))
	require.ErrorIs(t, err, errKV)
}

func TestKVRaftStoreFirstIndexReturnsCloseError(t *testing.T) {
	errClose := errors.New("close failure")
	store := &kvRaftStore{kv: &fakeKV{
		get: func(string) (string, []byte, io.Closer, error) {
			return "log-00000000000000000001", nil, &fakeCloser{err: errClose}, nil
		},
	}}

	_, err := store.FirstIndex()
	require.ErrorIs(t, err, errClose)
}

func TestKVRaftStoreStoreLogsReturnsPutError(t *testing.T) {
	errPut := errors.New("put failure")
	batch := &fakeWriteBatch{putErr: errPut}
	store := &kvRaftStore{kv: &fakeKV{batch: batch}}

	require.ErrorIs(t, store.StoreLog(&hashicorpraft.Log{Index: 1}), errPut)
	assert.True(t, batch.closed)
}

func TestKVRaftStoreStoreLogsReturnsEncodeError(t *testing.T) {
	batch := &fakeWriteBatch{}
	store := &kvRaftStore{kv: &fakeKV{batch: batch}}

	err := store.StoreLog(&hashicorpraft.Log{Index: 1, AppendedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)})
	require.ErrorContains(t, err, "year outside of range")
	assert.True(t, batch.closed)
}

func TestKVRaftStoreDeleteRange(t *testing.T) {
	store := newTestStore(t)
	storeLogs(t, store, 1, 2, 3, 4, 5)

	require.NoError(t, store.DeleteRange(1, 3))

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 4, first)
}

// maxInclusive+1 overflows when maxInclusive is MaxUint64, inverting the
// range into a no-op that silently deletes nothing.
func TestKVRaftStoreDeleteRangeMaxUint64(t *testing.T) {
	store := newTestStore(t)
	storeLogs(t, store, 1, 2, 3)

	require.NoError(t, store.DeleteRange(0, math.MaxUint64))

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 0, first)
	last, err := store.LastIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 0, last)
}

func TestKVRaftStoreStableStore(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.Set([]byte("key"), []byte("value")))
	value, err := store.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)

	require.NoError(t, store.SetUint64([]byte("counter"), 42))
	n, err := store.GetUint64([]byte("counter"))
	require.NoError(t, err)
	assert.EqualValues(t, 42, n)
}

func TestKVRaftStoreGetUint64RejectsInvalidValue(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.Set([]byte("counter"), []byte("abc")))

	_, err := store.GetUint64([]byte("counter"))
	require.ErrorContains(t, err, "invalid value of counter")
}

// fakeKV implements the methods of the kv the store uses: the others panic.
type fakeKV struct {
	kvstore.KV

	get   func(key string) (string, []byte, io.Closer, error)
	batch kvstore.WriteBatch
}

func (f *fakeKV) Get(key string, _ kvstore.ComparisonType, _ kvstore.IteratorOpts) (string, []byte, io.Closer, error) {
	return f.get(key)
}

func (f *fakeKV) NewWriteBatch() kvstore.WriteBatch {
	return f.batch
}

// fakeWriteBatch implements the methods of the batch the store uses: the
// others panic.
type fakeWriteBatch struct {
	kvstore.WriteBatch

	putErr error
	closed bool
}

func (b *fakeWriteBatch) Put(string, []byte) error {
	return b.putErr
}

func (b *fakeWriteBatch) Close() error {
	b.closed = true
	return nil
}

type fakeCloser struct {
	err error
}

func (c *fakeCloser) Close() error {
	return c.err
}
