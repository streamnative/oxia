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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestOpenSnapshotReaderFailsOnUnknownKeyEncoding(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(path, markerFileName), []byte("unknown"), 0o600))

	reader, err := OpenSnapshotReader(path)
	assert.ErrorContains(t, err, "failed to read the key encoding of the snapshot")
	assert.Nil(t, reader)
}

func TestOpenSnapshotReaderFailsWithoutDatabase(t *testing.T) {
	reader, err := OpenSnapshotReader(t.TempDir())
	assert.ErrorContains(t, err, "failed to open the snapshot")
	assert.Nil(t, reader)
}

func TestSnapshotReaderGetRejectsRangeReads(t *testing.T) {
	reader := openTestSnapshotReader(t, newSeekScanKV(t, proto.KeySortingType_NATURAL))

	_, _, _, err := reader.Get("a", ComparisonFloor, IteratorOpts{})
	assert.ErrorContains(t, err, "only supports point reads")
}

func TestSnapshotReaderGetReturnsReadError(t *testing.T) {
	reader := openTestSnapshotReader(t, newKVWithUnreadableSST(t))

	_, value, closer, err := reader.Get("b", ComparisonEqual, IteratorOpts{})
	require.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.NoError(t, closer.Close())

	_, _, _, err = reader.Get("c", ComparisonEqual, IteratorOpts{})
	assert.ErrorIs(t, err, os.ErrPermission)
}

// openTestSnapshotReader opens a reader on a snapshot of kv, which share its
// files, and closes them at the end of the test.
func openTestSnapshotReader(t *testing.T, kv KV) SnapshotReader {
	t.Helper()
	snapshot, err := kv.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, snapshot.Close()) })
	reader, err := OpenSnapshotReader(snapshot.BasePath())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, reader.Close()) })
	return reader
}
