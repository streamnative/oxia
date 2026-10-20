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

package validate

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdAcceptsTheShippedConfigurations(t *testing.T) {
	for _, name := range []string{"dataserver", "coordinator"} {
		out, err := runValidate(t, "--component", name, filepath.Join("..", "..", "..", "conf", name+".yaml"))

		require.NoError(t, err, name)
		assert.Contains(t, out, "is a valid "+name+" configuration", name)
	}
}

func TestCmdRejectsUnknownKeys(t *testing.T) {
	file := writeConfig(t, "server:\n  public:\n    bindAdress: 0.0.0.0:6648\n")

	_, err := runValidate(t, file)

	assert.ErrorContains(t, err, "invalid dataserver configuration "+file)
	assert.ErrorContains(t, err, "field bindAdress not found")
}

func TestCmdRejectsInvalidValues(t *testing.T) {
	file := writeConfig(t, "metadata:\n  providerName: zookeeper\n")

	_, err := runValidate(t, "--component", "coordinator", file)

	assert.ErrorContains(t, err, "invalid coordinator configuration "+file)
	assert.ErrorContains(t, err, `must be one of "memory", "configmap", "raft" or "file"`)
}

func TestCmdRejectsMissingFile(t *testing.T) {
	_, err := runValidate(t, filepath.Join(t.TempDir(), "missing.yaml"))

	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestCmdRejectsUnknownComponent(t *testing.T) {
	_, err := runValidate(t, "--component", "broker", writeConfig(t, ""))

	assert.EqualError(t, err, `unknown component "broker": use dataserver or coordinator`)
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(file, []byte(content), 0o600))
	return file
}

func runValidate(t *testing.T, args ...string) (string, error) {
	t.Helper()
	t.Cleanup(func() { componentName = "dataserver" })
	var out bytes.Buffer
	Cmd.SetOut(&out)
	Cmd.SetArgs(args)
	err := Cmd.Execute()
	return out.String(), err
}
