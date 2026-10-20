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

//revive:disable-next-line:var-naming
package common

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWriteOutput(t *testing.T) {
	for _, test := range []struct {
		name     string
		result   any
		expected string
	}{
		{"common.OutputError", OutputError{Err: "hello"}, "{\"error\":\"hello\"}\n"},
		{"common.OutputErrorEmpty", OutputError{}, "{}\n"},
		{"common.OutputVersion", OutputVersion{
			Key:                "my-key",
			VersionId:          1,
			CreatedTimestamp:   time.UnixMilli(2),
			ModifiedTimestamp:  time.UnixMilli(3),
			ModificationsCount: 1,
			SessionId:          5,
		}, "{\"key\":\"my-key\",\"version_id\":1,\"created_timestamp\":\"" + time.UnixMilli(2).Format(time.RFC3339Nano) +
			"\",\"modified_timestamp\":\"" + time.UnixMilli(3).Format(time.RFC3339Nano) +
			"\",\"modifications_count\":1,\"ephemeral\":false,\"session_id\":5,\"client_identity\":\"\"}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := bytes.NewBufferString("")
			WriteOutput(b, test.result)
			assert.Equal(t, test.expected, b.String())
		})
	}
}

func TestWriteOutputWritesStringsAndBytes(t *testing.T) {
	b := bytes.NewBufferString("")
	WriteOutput(b, "a")
	WriteOutput(b, []string{"b", "c"})
	WriteOutput(b, []byte("d"))
	assert.Equal(t, "a\nb\nc\nd\n", b.String())
}

func TestWriteOutputPanicsOnMarshalError(t *testing.T) {
	assert.Panics(t, func() { WriteOutput(bytes.NewBufferString(""), make(chan int)) })
}

func TestWriteOutputPanicsOnWriteError(t *testing.T) {
	for _, test := range []struct {
		name          string
		value         any
		successWrites int
	}{
		{"json", OutputError{Err: "hello"}, 0},
		{"string", "a", 0},
		{"string-newline", "a", 1},
		{"string-slice", []string{"a"}, 0},
		{"bytes", []byte("a"), 0},
		{"bytes-newline", []byte("a"), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := &failingWriter{successWrites: test.successWrites}
			assert.PanicsWithError(t, errWrite.Error(), func() { WriteOutput(w, test.value) })
		})
	}
}

func TestWriteHexDump(t *testing.T) {
	b := bytes.NewBufferString("")
	WriteHexDump(b, []byte("abc"))
	assert.Equal(t, "00000000  61 62 63                                          |abc|\n", b.String())
}

func TestWriteHexDumpPanicsOnWriteError(t *testing.T) {
	w := &failingWriter{}
	assert.PanicsWithError(t, errWrite.Error(), func() { WriteHexDump(w, []byte("abc")) })
}

func TestWriteHexDumpPanicsOnCloseError(t *testing.T) {
	// The dump of a partial line is completed when the dumper closes: the
	// writes of the line itself succeed, and only the completion fails
	w := &failingWriter{successWrites: countWrites(t, []byte("a"))}
	assert.PanicsWithError(t, errWrite.Error(), func() { WriteHexDump(w, []byte("a")) })
}

var errWrite = errors.New("write failed")

// failingWriter accepts the first successWrites writes, then fails all the
// others.
type failingWriter struct {
	successWrites int
	writes        int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > w.successWrites {
		return 0, errWrite
	}
	return len(p), nil
}

// countWrites returns how many writes the hex dumper makes for buffer before
// it closes.
func countWrites(t *testing.T, buffer []byte) int {
	t.Helper()
	w := &failingWriter{successWrites: 1 << 30}
	dump := hex.Dumper(w)
	_, err := dump.Write(buffer)
	assert.NoError(t, err)
	return w.writes
}
