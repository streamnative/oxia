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

package process

import (
	"bytes"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunProfilingEnablesContentionProfiles(t *testing.T) {
	PprofEnable = true
	PprofBindAddress = "127.0.0.1:0"
	t.Cleanup(func() {
		PprofEnable = false
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	})

	closer := RunProfiling(t.Context())

	// SetMutexProfileFraction with a negative value only reads the current
	// setting. There is no equivalent getter for the block profile rate.
	assert.Equal(t, mutexProfileFraction, runtime.SetMutexProfileFraction(-1))

	// Closing the profiling server restores the runtime defaults
	assert.NoError(t, closer.Close())
	assert.Equal(t, 0, runtime.SetMutexProfileFraction(-1))
}

func TestRunProfilingDisabledDoesNotStartServer(t *testing.T) {
	PprofEnable = false
	PprofBindAddress = "127.0.0.1:0"

	closer := RunProfiling(t.Context())

	// The plain server is returned without listening, and contention profiling stays off
	server, ok := closer.(*http.Server)
	require.True(t, ok)
	assert.Equal(t, "127.0.0.1:0", server.Addr)
	assert.Equal(t, 0, runtime.SetMutexProfileFraction(-1))
	assert.NoError(t, closer.Close())
}

func TestRunProfilingLogsListenFailure(t *testing.T) {
	logs := captureLogs(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	PprofEnable = true
	PprofBindAddress = listener.Addr().String()
	t.Cleanup(func() {
		PprofEnable = false
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	})

	closer := RunProfiling(t.Context())

	assert.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "Unable to start debug profiling server")
	}, 10*time.Second, 10*time.Millisecond)
	assert.NoError(t, closer.Close())
}

// captureLogs routes the default logger to the returned buffer until the test ends.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	logs := &syncBuffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	return logs
}

// syncBuffer is a buffer that a goroutine's logger writes while the test reads it.
type syncBuffer struct {
	sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.Lock()
	defer b.Unlock()
	return b.buf.String()
}
