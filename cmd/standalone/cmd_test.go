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

package standalone

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestStandalone_RejectsPositionalArgs(t *testing.T) {
	run := Cmd.RunE
	t.Cleanup(func() { Cmd.RunE = run })
	invoked := false
	Cmd.RunE = func(*cobra.Command, []string) error { invoked = true; return nil }

	// A bool flag never takes the next argument: "false" is a stray positional and the WAL sync stays on
	Cmd.SetArgs([]string{"--wal-sync-data", "false"})
	err := Cmd.Execute()

	assert.ErrorContains(t, err, `unknown command "false" for "standalone"`)
	assert.False(t, invoked)
}

func TestStandalonePreRunEAppliesFlags(t *testing.T) {
	run := Cmd.RunE
	t.Cleanup(func() {
		Cmd.RunE = run
		_ = Cmd.Flags().Parse([]string{"--key-sorting", "hierarchical"})
	})
	invoked := false
	Cmd.RunE = func(*cobra.Command, []string) error { invoked = true; return nil }

	Cmd.SetArgs([]string{"--key-sorting", "natural", "--wal-retention-time", "5m",
		"--notifications-retention-time", "7m"})
	err := Cmd.Execute()

	require.NoError(t, err)
	assert.True(t, invoked)
	assert.Equal(t, proto.KeySortingType_NATURAL, conf.KeySorting)
	assert.Equal(t, 5*time.Minute, time.Duration(conf.DataServerOptions.Storage.WAL.Retention))
	assert.Equal(t, 7*time.Minute, time.Duration(conf.DataServerOptions.Storage.Notification.Retention))
}

func TestStandalonePreRunERejectsInvalidKeySorting(t *testing.T) {
	run := Cmd.RunE
	t.Cleanup(func() {
		Cmd.RunE = run
		_ = Cmd.Flags().Parse([]string{"--key-sorting", "hierarchical"})
	})
	invoked := false
	Cmd.RunE = func(*cobra.Command, []string) error { invoked = true; return nil }
	Cmd.SetOut(new(bytes.Buffer))
	Cmd.SetErr(new(bytes.Buffer))

	Cmd.SetArgs([]string{"--key-sorting", "random"})
	err := Cmd.Execute()

	require.EqualError(t, err, `must be one of "natural" or "hierarchical"`)
	assert.False(t, invoked)
}

func TestStandaloneKeySortingCompletion(t *testing.T) {
	out := new(bytes.Buffer)
	root := &cobra.Command{Use: "oxia"}
	root.AddCommand(Cmd)
	t.Cleanup(func() { root.RemoveCommand(Cmd) })
	// The completion is written to the output of the completed command
	Cmd.SetOut(out)
	Cmd.SetErr(new(bytes.Buffer))

	root.SetArgs([]string{cobra.ShellCompRequestCmd, "standalone", "--key-sorting", ""})
	require.NoError(t, root.Execute())

	assert.Contains(t, out.String(), "hierarchical\tUse file-system like hierarchical sorting based on `/`")
	assert.Contains(t, out.String(), "natural\tUse natural, byte-wise sorting")
}

func TestStandaloneRunsUntilItsContextIsDone(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	Cmd.SetArgs([]string{"--data-dir", filepath.Join(dir, "db"), "--wal-dir", filepath.Join(dir, "wal"),
		"--public-addr", "localhost:0", "--metrics-addr", "localhost:0"})

	assert.NoError(t, Cmd.ExecuteContext(ctx))
}

func TestStandaloneFailsWhenItCantStart(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(dataDir, nil, 0o600))

	Cmd.SetArgs([]string{"--data-dir", dataDir, "--wal-dir", t.TempDir(),
		"--public-addr", "localhost:0", "--metrics-addr", "localhost:0"})

	assert.ErrorContains(t, Cmd.ExecuteContext(t.Context()), "failed to start the process")
}
