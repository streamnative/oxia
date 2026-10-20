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

package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
)

func TestWatchFileSignalsWriteAndClosesOnCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changes, err := WatchFile(ctx, path)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("a: 1"), 0o600))
	select {
	case _, ok := <-changes:
		assert.True(t, ok)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no change signalled for the written file")
	}

	cancel()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-changes:
			return !ok
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
}

func TestWatchFileReturnsErrorForMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "config.yaml")

	changes, err := WatchFile(context.Background(), path)

	assert.Error(t, err)
	assert.Nil(t, changes)
}

func TestWatchFileReturnsWatcherCreationError(t *testing.T) {
	injected := errors.New("injected")
	failpoints := failpoint.New(map[string]func() error{
		"watchFile.newWatcher": func() error { return injected },
	})

	changes, err := watchFile(t.Context(), filepath.Join(t.TempDir(), "config.yaml"), failpoints)

	assert.ErrorIs(t, err, injected)
	assert.Nil(t, changes)
}

func TestWatchFileClosesWhenTheWatchFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	failpoints := failpoint.New(map[string]func() error{
		"watchFile.event": func() error { return errors.New("injected") },
	})

	changes, err := watchFile(t.Context(), path, failpoints)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("a: 1"), 0o600))
	assertClosed(t, changes)
}

func TestWatchFileClosesWhenTheWatcherFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	paused := make(chan struct{})
	release := make(chan struct{})
	var pause sync.Once
	failpoints := failpoint.New(map[string]func() error{
		"watchFile.event": func() error {
			pause.Do(func() {
				close(paused)
				<-release
			})
			return nil
		},
	})

	changes, err := watchFile(t.Context(), path, failpoints)
	require.NoError(t, err)

	switch runtime.GOOS {
	case "linux":
		// inotify reports an overflow once the events that nobody reads pass
		// its queue limit
		require.NoError(t, os.WriteFile(filepath.Join(dir, "first"), nil, 0o600))
		select {
		case <-paused:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "no event for the written file")
		}
		data, err := os.ReadFile("/proc/sys/fs/inotify/max_queued_events")
		require.NoError(t, err)
		limit, err := strconv.Atoi(strings.TrimSpace(string(data)))
		require.NoError(t, err)
		// The watcher reads a batch of the events from the queue before it
		// waits for the watch to take them
		for i := range limit + 4096 {
			require.NoError(t, os.WriteFile(filepath.Join(dir, strconv.Itoa(i)), nil, 0o600))
		}
	case "darwin", "dragonfly", "freebsd", "netbsd", "openbsd":
		// kqueue reports the failure to list a directory whose content changed
		require.NoError(t, os.Chmod(dir, 0o300))
		t.Cleanup(func() { assert.NoError(t, os.Chmod(dir, 0o700)) })
		require.NoError(t, os.WriteFile(path, nil, 0o600))
	default:
		t.Skipf("no way to fail the watcher on %s", runtime.GOOS)
	}
	close(release)

	assertClosed(t, changes)
}

func TestWatchFileClosesWhenTheWatcherFailsToClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	ctx, cancel := context.WithCancel(t.Context())
	closed := make(chan struct{})
	failpoints := failpoint.New(map[string]func() error{
		"watchFile.close": func() error {
			close(closed)
			return errors.New("injected")
		},
	})

	changes, err := watchFile(ctx, path, failpoints)
	require.NoError(t, err)

	cancel()
	assertClosed(t, changes)
	select {
	case <-closed:
	default:
		assert.Fail(t, "the watcher was not closed")
	}
}

func assertClosed(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-changes:
		assert.False(t, ok)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the changes channel was not closed")
	}
}
