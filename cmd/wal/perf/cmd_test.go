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
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/common/proto"
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

func TestRunReturnsOutputErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		writes int
	}{
		{name: "thread", writes: 0},
		{name: "histogram", writes: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			setWalOption(t, t.TempDir(), "default")
			out := &failingWriter{writes: test.writes, err: errors.New("output closed")}
			Cmd.SetOut(out)
			t.Cleanup(func() { Cmd.SetOut(nil) })
			Cmd.SetArgs([]string{"--entry-count", "1", "--reader-count", "1", "--sync-data=false"})

			assert.ErrorIs(t, Cmd.Execute(), out.err)
		})
	}
}

func TestRunPanicsOnInvalidNamespace(t *testing.T) {
	setWalOption(t, t.TempDir(), "../ns")

	assert.PanicsWithError(t, `namespace "../ns" contains invalid path traversal sequence`, func() {
		Cmd.SetArgs([]string{"--entry-count", "1"})
		_ = Cmd.Execute()
	})
}

func TestRunWriterPanicsOnAppendError(t *testing.T) {
	setEntryCount(t, 1)
	w := &fakeWal{appendErr: errors.New("append failed")}

	assert.PanicsWithError(t, "append failed", func() {
		runWriter(w, []byte("a"), make([]int64, 1), make([]int64, 1))
	})
}

func TestRunReaderPanicsOnNewReaderError(t *testing.T) {
	setEntryCount(t, 1)
	w := &fakeWal{readerErr: errors.New("reader failed")}

	assert.PanicsWithError(t, "reader failed", func() {
		runReader(w, 0, make([]int64, 2), make([]int64, 1))
	})
}

func TestRunReaderPanicsOnReadError(t *testing.T) {
	setEntryCount(t, 1)
	w := &fakeWal{reader: &fakeReader{err: errors.New("read failed")}}

	assert.PanicsWithError(t, "read failed", func() {
		runReader(w, 0, make([]int64, 2), make([]int64, 1))
	})
}

func setEntryCount(t *testing.T, entryCount int64) {
	t.Helper()
	previous := options.entryCount
	options.entryCount = entryCount
	t.Cleanup(func() { options.entryCount = previous })
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
	stdout := &bytes.Buffer{}
	Cmd.SetOut(stdout)
	t.Cleanup(func() { Cmd.SetOut(nil) })
	Cmd.SetArgs(args)
	err := Cmd.Execute()
	return stdout.String(), err
}

// failingWriter accepts the given number of writes, and fails the next ones.
type failingWriter struct {
	writes int
	err    error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.writes == 0 {
		return 0, w.err
	}
	w.writes--
	return len(p), nil
}

type fakeWal struct {
	wal.Wal
	appendErr error
	reader    wal.Reader
	readerErr error
}

func (w *fakeWal) AppendAndSync(_ *proto.LogEntry, callback func(entryCrc uint32, err error)) {
	callback(0, w.appendErr)
}

func (w *fakeWal) NewReader(int64) (wal.Reader, error) {
	return w.reader, w.readerErr
}

type fakeReader struct {
	wal.Reader
	err error
}

func (*fakeReader) HasNext() bool {
	return true
}

func (r *fakeReader) ReadNext() (entry *proto.LogEntry, previousCrc uint32, entryCrc uint32, err error) {
	return nil, 0, 0, r.err
}
