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

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadJSONFromFile(t *testing.T) {
	t.Run("read existing json", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		require.NoError(t, os.WriteFile(path, []byte(`{"hello":"world"}`), 0o600))

		value := map[string]string{}
		hasContent, err := ReadJSONFromFile(afero.NewOsFs(), path, &value)
		require.NoError(t, err)
		assert.True(t, hasContent)
		assert.Equal(t, map[string]string{"hello": "world"}, value)
	})

	t.Run("empty file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		require.NoError(t, os.WriteFile(path, nil, 0o600))

		value := map[string]string{"existing": "value"}
		hasContent, err := ReadJSONFromFile(afero.NewOsFs(), path, &value)
		require.NoError(t, err)
		assert.False(t, hasContent)
		assert.Equal(t, map[string]string{"existing": "value"}, value)
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.json")

		value := map[string]string{}
		hasContent, err := ReadJSONFromFile(afero.NewOsFs(), path, &value)
		require.Error(t, err)
		assert.True(t, os.IsNotExist(err))
		assert.False(t, hasContent)
	})
}

func TestWriteJSONAtomicallyCreatesParentDirectoryAndFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")

	err := WriteJSONToFile(afero.NewOsFs(), path, map[string]string{
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

	err := WriteJSONToFile(afero.NewOsFs(), filepath.Join(parent, "state.json"), map[string]string{})
	assert.ErrorIs(t, err, syscall.ENOTDIR)
}

func TestWriteJSONToFileUnsupportedValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	err := WriteJSONToFile(afero.NewOsFs(), path, make(chan int))
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

	err := WriteJSONToFile(afero.NewOsFs(), filepath.Join(dir, "state.json"), map[string]string{})
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

	err := WriteJSONToFile(afero.NewOsFs(), path, map[string]string{"hello": "world"})
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

	err := WriteJSONToFile(afero.NewOsFs(), path, map[string]string{})
	var linkErr *os.LinkError
	assert.ErrorAs(t, err, &linkErr)
	// The temporary file is removed when the rename fails
	assertDirEntries(t, dir, "state.json")
}

func TestWriteJSONToFileReturnsTemporaryFileErrors(t *testing.T) {
	injected := errors.New("injected")
	for _, test := range []struct {
		name string
		fs   *faultFs
	}{
		{name: "write", fs: &faultFs{writeErr: injected}},
		{name: "sync", fs: &faultFs{syncErr: injected}},
		{name: "close", fs: &faultFs{closeErr: injected}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			test.fs.Fs = afero.NewOsFs()

			err := WriteJSONToFile(test.fs, filepath.Join(dir, "state.json"), map[string]string{})

			assert.ErrorIs(t, err, injected)
			assertDirEntries(t, dir)
		})
	}
}

func TestWriteJSONToFileReturnsTemporaryFileRemoveError(t *testing.T) {
	removeErr := errors.New("remove failed")
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	err := WriteJSONToFile(&faultFs{Fs: afero.NewOsFs(), removeErr: removeErr}, path, map[string]string{"hello": "world"})

	assert.ErrorIs(t, err, removeErr)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.JSONEq(t, `{"hello":"world"}`, string(content))
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

type faultFs struct {
	afero.Fs
	writeErr  error
	syncErr   error
	closeErr  error
	removeErr error
}

func (f *faultFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

func (f *faultFs) Remove(name string) error {
	if f.removeErr != nil {
		return f.removeErr
	}
	return f.Fs.Remove(name)
}

type faultFile struct {
	afero.File
	fs *faultFs
}

func (f *faultFile) Write(p []byte) (int, error) {
	if f.fs.writeErr != nil {
		return 0, f.fs.writeErr
	}
	return f.File.Write(p)
}

func (f *faultFile) Sync() error {
	if f.fs.syncErr != nil {
		return f.fs.syncErr
	}
	return f.File.Sync()
}

func (f *faultFile) Close() error {
	if err := f.File.Close(); err != nil {
		return err
	}
	return f.fs.closeErr
}
