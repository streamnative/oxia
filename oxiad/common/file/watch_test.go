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
	"os"
	"path/filepath"
	"testing"
	"time"

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
