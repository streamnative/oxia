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

package version

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/common/version"
)

func TestCmdPrintsText(t *testing.T) {
	out, err := runVersion(t, "1.2.3")

	require.NoError(t, err)
	assert.Equal(t, version.Get("1.2.3").String(), out)
}

func TestCmdPrintsJSON(t *testing.T) {
	out, err := runVersion(t, "1.2.3", "-o", "json")

	require.NoError(t, err)
	var info version.Info
	require.NoError(t, json.Unmarshal([]byte(out), &info))
	assert.Equal(t, version.Get("1.2.3"), info)
}

func TestCmdRejectsUnknownOutput(t *testing.T) {
	_, err := runVersion(t, "1.2.3", "-o", "yaml")

	assert.ErrorContains(t, err, `unknown output format "yaml": use text or json`)
}

func runVersion(t *testing.T, rootVersion string, args ...string) (string, error) {
	t.Helper()
	t.Cleanup(func() { output = "text" })
	root := &cobra.Command{Use: "oxia", Version: rootVersion}
	root.AddCommand(Cmd)
	t.Cleanup(func() { root.RemoveCommand(Cmd) })
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs(append([]string{"version"}, args...))
	err := root.Execute()
	return out.String(), err
}
