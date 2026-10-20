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

package createschema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdWritesSchemas(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, runCreateSchema(t, dir))

	for _, name := range []string{"coordinator.json", "dataserver.json"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		var schema map[string]any
		require.NoError(t, json.Unmarshal(data, &schema), name)
		assert.Contains(t, schema, "$schema", name)
		assert.Contains(t, schema, "$defs", name)
	}
}

func TestCmdReturnsCoordinatorWriteError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")

	err := runCreateSchema(t, dir)

	require.ErrorIs(t, err, os.ErrNotExist)
	assert.NoDirExists(t, dir)
}

func TestCmdReturnsDataServerWriteError(t *testing.T) {
	dir := t.TempDir()
	// A directory in place of the data server schema makes only its write fail
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dataserver.json"), 0o700))

	err := runCreateSchema(t, dir)

	require.ErrorContains(t, err, "is a directory")
	assert.FileExists(t, filepath.Join(dir, "coordinator.json"))
}

func runCreateSchema(t *testing.T, dir string) error {
	t.Helper()
	t.Cleanup(func() { outputDir = "" })
	Cmd.SetArgs([]string{"--output-dir", dir})
	return Cmd.Execute()
}
