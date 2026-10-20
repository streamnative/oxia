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
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunProcessClosesTheProcessWhenItsContextIsDone(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(t.Context())
	process := &testCloser{}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- RunProcess(ctx, "TEST", func() (io.Closer, error) {
			close(started)
			return process, nil
		})
	}()

	<-started
	assert.False(t, process.closed)
	cancel()
	assert.NoError(t, <-done)
	assert.True(t, process.closed)
	assert.Contains(t, logs.String(), "Oxia TEST is ready")
	assert.Contains(t, logs.String(), "Oxia TEST is shutting down")
	assert.Contains(t, logs.String(), "Oxia TEST stopped")
}

func TestRunProcessFailsWhenTheProcessFailsToStart(t *testing.T) {
	startErr := errors.New("start failure")

	err := RunProcess(t.Context(), "TEST", func() (io.Closer, error) { return nil, startErr })

	assert.ErrorIs(t, err, startErr)
	assert.ErrorContains(t, err, "failed to start the process")
}

func TestRunProcessFailsWhenTheProcessFailsToClose(t *testing.T) {
	closeErr := errors.New("close failure")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := RunProcess(ctx, "TEST", func() (io.Closer, error) { return &testCloser{err: closeErr}, nil })

	assert.ErrorIs(t, err, closeErr)
	assert.ErrorContains(t, err, "failed to shut down the process")
}

type testCloser struct {
	closed bool
	err    error
}

func (c *testCloser) Close() error {
	c.closed = true
	return c.err
}
