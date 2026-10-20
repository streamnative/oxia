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

package kvstore

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
)

func TestPebbleSnapshotNewFailsWhenCheckpointDirectoryExists(t *testing.T) {
	kv := newSeekScanKV(t, proto.KeySortingType_NATURAL)
	next := filepath.Join(kv.factory.dataDir, validation.KeywordNamespaceSnapshots, "shard-1", "snapshot-1")
	require.NoError(t, os.MkdirAll(next, 0o755))

	_, err := kv.Snapshot()
	assert.Error(t, err)
}

func TestPebbleSnapshotNewFailsWhenMarkerIsMissing(t *testing.T) {
	kv := newSeekScanKV(t, proto.KeySortingType_NATURAL)
	require.NoError(t, os.Remove(filepath.Join(kv.dbPath, markerFileName)))

	_, err := kv.Snapshot()
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestPebbleSnapshotNewFailsOnFilesystemErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		kind errorfs.OpKind
	}{
		{"list", errorfs.OpList},
		{"stat", errorfs.OpStat},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataDir := t.TempDir()
			snapshotPath := filepath.Join(dataDir, validation.KeywordNamespaceSnapshots, "shard-1", "snapshot-1")
			// The checkpoint lists and stats the snapshot too: fail only the
			// operations that follow the link of the marker
			var armed atomic.Bool
			factory := newFactoryWithFaults(t, dataDir, func(op errorfs.Op) bool {
				if op.Kind == errorfs.OpLink && filepath.Base(op.Path) == markerFileName {
					armed.Store(true)
				}
				return armed.Load() && op.Kind == test.kind && strings.HasPrefix(op.Path, snapshotPath)
			})
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, kv.Close()) })

			_, err = kv.Snapshot()
			assert.ErrorIs(t, err, errorfs.ErrInjected)
		})
	}
}

func TestPebbleSnapshotNewFailsWhenFlushFails(t *testing.T) {
	factory, kv := newKVWithFailpoint(t, "snapshot.flush")

	snapshot, err := kv.Snapshot()
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, snapshot)
	assert.NoDirExists(t, filepath.Join(factory.dataDir, validation.KeywordNamespaceSnapshots, "shard-1"))
	assert.NoError(t, kv.Close())
}

// Closing a snapshot in the middle of sending a file closes the file.
func TestPebbleSnapshotCloseClosesPartiallyReadFile(t *testing.T) {
	kv := newSeekScanKV(t, proto.KeySortingType_NATURAL)
	// Incompressible values, for a sstable larger than a chunk
	wb := kv.NewWriteBatch()
	value := make([]byte, MaxSnapshotChunkSize/4)
	for _, key := range []string{"big-1", "big-2", "big-3", "big-4", "big-5", "big-6"} {
		_, err := rand.Read(value)
		require.NoError(t, err)
		require.NoError(t, wb.Put(key, value))
	}
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())

	snapshot, err := kv.Snapshot()
	require.NoError(t, err)
	var chunk SnapshotChunk
	for ; snapshot.Valid(); snapshot.Next() {
		if chunk, err = snapshot.Chunk(); err != nil || chunk.TotalCount() > 1 {
			break
		}
	}
	require.NoError(t, err)
	require.Greater(t, chunk.TotalCount(), int32(1), "no file spans several chunks")
	file := snapshot.(*pebbleSnapshot).file
	require.NotNil(t, file)

	assert.NoError(t, snapshot.Close())
	assert.ErrorIs(t, file.Close(), os.ErrClosed)
	assert.NoDirExists(t, snapshot.BasePath())
}

func TestPebbleSnapshotChunkFailsWhenFileIsMissing(t *testing.T) {
	kv := newSeekScanKV(t, proto.KeySortingType_NATURAL)
	snapshot, err := kv.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, snapshot.Close()) })
	require.True(t, snapshot.Valid())
	require.NoError(t, os.Remove(filepath.Join(snapshot.BasePath(), snapshot.(*pebbleSnapshot).files[0])))

	_, err = snapshot.Chunk()
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestPebbleSnapshotChunkFailsWhenFileCannotBeOpened(t *testing.T) {
	kv := newSeekScanKV(t, proto.KeySortingType_NATURAL)
	snapshot, err := kv.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, snapshot.Close()) })
	require.True(t, snapshot.Valid())
	// The snapshot files can be hard links to the database ones: copy the
	// file, to leave the database readable
	path := filepath.Join(snapshot.BasePath(), snapshot.(*pebbleSnapshot).files[0])
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, content, 0))
	if f, err := os.Open(path); err == nil {
		assert.NoError(t, f.Close())
		t.Skip("the file is still readable, as when running as root")
	}

	_, err = snapshot.Chunk()
	assert.ErrorIs(t, err, os.ErrPermission)
}

func TestPebbleSnapshotChunkFailsOnFilesystemErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		kind errorfs.OpKind
	}{
		{"read", errorfs.OpFileReadAt},
		{"close", errorfs.OpFileClose},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataDir := t.TempDir()
			snapshotsDir := filepath.Join(dataDir, validation.KeywordNamespaceSnapshots)
			// The checkpoint closes the files it writes: fail only the
			// reads of the snapshot that follow it
			var armed atomic.Bool
			factory := newFactoryWithFaults(t, dataDir, func(op errorfs.Op) bool {
				return armed.Load() && op.Kind == test.kind && strings.HasPrefix(op.Path, snapshotsDir)
			})
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, kv.Close()) })
			snapshot, err := kv.Snapshot()
			require.NoError(t, err)
			t.Cleanup(func() {
				armed.Store(false)
				assert.NoError(t, snapshot.Close())
			})
			require.True(t, snapshot.Valid())

			armed.Store(true)
			_, err = snapshot.Chunk()
			assert.ErrorIs(t, err, errorfs.ErrInjected)
		})
	}
}

func TestPebbleSnapshotChunkClosesFileOfChunkMultipleSize(t *testing.T) {
	var opened, closed int
	injector := errorfs.InjectorFunc(func(op errorfs.Op) error {
		switch op.Kind {
		case errorfs.OpOpen:
			opened++
		case errorfs.OpFileClose:
			closed++
		default:
		}
		return nil
	})
	memFS := vfs.NewMem()
	snapshot := &pebbleSnapshot{
		fs:    closeFaultFS{FS: errorfs.Wrap(memFS, injector), injector: injector},
		path:  "snapshot",
		files: []string{"chunk-multiple", "small"},
	}
	require.NoError(t, memFS.MkdirAll(snapshot.path, 0o755))
	for name, size := range map[string]int64{"chunk-multiple": 2 * MaxSnapshotChunkSize, "small": 10} {
		f, err := memFS.Create(filepath.Join(snapshot.path, name), vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		_, err = f.Write(make([]byte, size))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	var chunks int
	for ; snapshot.Valid(); snapshot.Next() {
		_, err := snapshot.Chunk()
		require.NoError(t, err)
		chunks++
	}

	assert.Equal(t, 3, chunks)
	assert.Equal(t, 2, opened)
	assert.Equal(t, opened, closed)
	assert.NoError(t, snapshot.Close())
}
