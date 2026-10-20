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

package commonio

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
)

func TestReadJSONFromFile(t *testing.T) {
	t.Run("read existing json", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		require.NoError(t, os.WriteFile(path, []byte(`{"hello":"world"}`), 0o600))

		value := map[string]string{}
		hasContent, err := ReadJSONFromFile(path, &value)
		require.NoError(t, err)
		assert.True(t, hasContent)
		assert.Equal(t, map[string]string{"hello": "world"}, value)
	})

	t.Run("empty file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		require.NoError(t, os.WriteFile(path, nil, 0o600))

		value := map[string]string{"existing": "value"}
		hasContent, err := ReadJSONFromFile(path, &value)
		require.NoError(t, err)
		assert.False(t, hasContent)
		assert.Equal(t, map[string]string{"existing": "value"}, value)
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.json")

		value := map[string]string{}
		hasContent, err := ReadJSONFromFile(path, &value)
		require.Error(t, err)
		assert.True(t, os.IsNotExist(err))
		assert.False(t, hasContent)
	})
}

func TestWriteJSONAtomicallyCreatesParentDirectoryAndFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")

	err := WriteJSONToFile(path, map[string]string{
		"hello": "world",
	})
	require.NoError(t, err)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.JSONEq(t, `{"hello":"world"}`, string(content))
}

func TestWriteJSONToFileParentIsFile(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "parent")
	require.NoError(t, os.WriteFile(parent, nil, 0o600))

	err := WriteJSONToFile(filepath.Join(parent, "state.json"), map[string]string{})
	assert.ErrorIs(t, err, syscall.ENOTDIR)
}

func TestWriteJSONToFileUnsupportedValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	err := WriteJSONToFile(path, make(chan int))
	var unsupported *json.UnsupportedTypeError
	assert.ErrorAs(t, err, &unsupported)
	assertDirEntries(t, dir)
}

func TestWriteJSONToFileReadOnlyDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := WriteJSONToFile(filepath.Join(dir, "state.json"), map[string]string{})
	assert.ErrorIs(t, err, os.ErrPermission)
	assertDirEntries(t, dir)
}

func TestWriteJSONToFileUnreadableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// Write and search permissions let the file be renamed into place, but the directory can't be opened to sync it
	require.NoError(t, os.Chmod(dir, 0o300))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := WriteJSONToFile(path, map[string]string{"hello": "world"})
	assert.ErrorIs(t, err, os.ErrPermission)

	require.NoError(t, os.Chmod(dir, 0o700))
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.JSONEq(t, `{"hello":"world"}`, string(content))
}

func TestWriteJSONToFileTargetIsDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	require.NoError(t, os.MkdirAll(filepath.Join(path, "child"), 0o755))

	err := WriteJSONToFile(path, map[string]string{})
	var linkErr *os.LinkError
	assert.ErrorAs(t, err, &linkErr)
	// The temporary file is removed when the rename fails
	assertDirEntries(t, dir, "state.json")
}

func TestWriteJSONToFileTempFileFailures(t *testing.T) {
	for _, name := range []string{"writeJSONToFile.write", "writeJSONToFile.sync", "writeJSONToFile.close"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.json")
			require.NoError(t, os.WriteFile(path, []byte(`{"hello":"world"}`), 0o600))
			injected := errors.New("injected")
			failpoints := failpoint.New(map[string]func() error{
				name: func() error { return injected },
			})

			err := writeJSONToFile(path, map[string]string{"hello": "oxia"}, failpoints)

			assert.ErrorIs(t, err, injected)
			assertDirEntries(t, dir, "state.json")
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.JSONEq(t, `{"hello":"world"}`, string(content))
		})
	}
}

func assertDirEntries(t *testing.T, dir string, expected ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	assert.ElementsMatch(t, expected, names)
}
