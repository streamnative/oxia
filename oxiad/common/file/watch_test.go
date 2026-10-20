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
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	path := filepath.Join(t.TempDir(), "config.yaml")
	var limit syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit))
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: 0, Max: limit.Max}))

	changes, err := WatchFile(context.Background(), path)

	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit))
	assert.ErrorIs(t, err, syscall.EMFILE)
	assert.Nil(t, changes)
}

func TestWatchFileReturnsAddAndCloseErrors(t *testing.T) {
	w := newFakeWatcher()
	w.addErr = errors.New("add failed")
	w.closeErr = errors.New("close failed")

	changes, err := watchFile(context.Background(), "config.yaml", w)

	assert.ErrorIs(t, err, w.addErr)
	assert.ErrorIs(t, err, w.closeErr)
	assert.Nil(t, changes)
}

func TestWatchFileClosesOnWatchError(t *testing.T) {
	w := newFakeWatcher()
	changes, err := watchFile(context.Background(), "config.yaml", w)
	require.NoError(t, err)

	w.errors <- errors.New("watch failed")

	assertClosed(t, changes)
	assert.True(t, w.closed)
}

func TestWatchFileClosesWhenWatcherCloses(t *testing.T) {
	for _, test := range []struct {
		name  string
		close func(w *fakeWatcher)
	}{
		{name: "errors", close: func(w *fakeWatcher) { close(w.errors) }},
		{name: "events", close: func(w *fakeWatcher) { close(w.events) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := newFakeWatcher()
			changes, err := watchFile(context.Background(), "config.yaml", w)
			require.NoError(t, err)

			test.close(w)

			assertClosed(t, changes)
		})
	}
}

func TestWatchFileClosesWhenWatcherFailsToClose(t *testing.T) {
	w := newFakeWatcher()
	w.closeErr = errors.New("close failed")
	ctx, cancel := context.WithCancel(context.Background())
	changes, err := watchFile(ctx, "config.yaml", w)
	require.NoError(t, err)

	cancel()

	assertClosed(t, changes)
	assert.True(t, w.closed)
}

func assertClosed(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-changes:
		assert.False(t, ok)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the changes were not closed")
	}
}

type fakeWatcher struct {
	addErr   error
	closeErr error
	events   chan fsnotify.Event
	errors   chan error
	closed   bool
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{events: make(chan fsnotify.Event), errors: make(chan error)}
}

func (w *fakeWatcher) Add(string) error {
	return w.addErr
}

func (w *fakeWatcher) Events() <-chan fsnotify.Event {
	return w.events
}

func (w *fakeWatcher) Errors() <-chan error {
	return w.errors
}

func (w *fakeWatcher) Close() error {
	w.closed = true
	return w.closeErr
}
