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

package perf

import (
	"io"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

func TestRunWritesAndReadsEntries(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")

	stdout, err := runPerf(t, "--entry-count", "20", "--entry-size", "8", "--reader-count", "2",
		"--segment-size", "256", "--sync-data=false")

	require.NoError(t, err)
	assert.Contains(t, stdout, "Thread: Writer, Total entry: 20")
	assert.Contains(t, stdout, "Thread: Reader0, Total entry: 20")
	assert.Contains(t, stdout, "Thread: Reader1, Total entry: 20")
	assert.Contains(t, stdout, "Histogram of Writer latency:")
	assert.Contains(t, stdout, "Histogram of Reader latency:")

	factory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: common.WalOption.WalDir, Retention: math.MaxInt64})
	w, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 19, w.LastOffset())
	require.NoError(t, w.Close())
	require.NoError(t, factory.Close())
}

func TestRunPanicsOnInvalidNamespace(t *testing.T) {
	setWalOption(t, t.TempDir(), "../ns")

	assert.PanicsWithError(t, `namespace "../ns" contains invalid path traversal sequence`, func() {
		Cmd.SetArgs([]string{"--entry-count", "1"})
		_ = Cmd.Execute()
	})
}

func setWalOption(t *testing.T, dir string, namespace string) {
	t.Helper()
	previous := common.WalOption
	common.WalOption = common.WalOptions{Namespace: namespace, Shard: 1, WalDir: dir}
	t.Cleanup(func() { common.WalOption = previous })
}

// runPerf runs the command and returns what it printed on stdout.
func runPerf(t *testing.T, args ...string) (string, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	stdout := os.Stdout
	os.Stdout = writer
	output := make(chan string)
	go func() {
		data, _ := io.ReadAll(reader)
		output <- string(data)
	}()

	Cmd.SetArgs(args)
	err = Cmd.Execute()

	os.Stdout = stdout
	require.NoError(t, writer.Close())
	result := <-output
	require.NoError(t, reader.Close())
	return result, err
}
