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

package client

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func TestExecutorRangeScanWritesHexDump(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyOptions []oxia.RangeScanOption
	mockClient.On("RangeScan", "a", "", emptyOptions).Return(scanResults(oxia.GetResult{Key: "a", Value: []byte("v")}))
	executor, out := newTestExecutor(mockClient)

	require.NoError(t, executor.Execute([]string{"range-scan", "--hex", "a"}))

	assert.Equal(t, "a\n"+hex.Dump([]byte("v")), out.String())
	mockClient.AssertExpectations(t)
}

func TestExecutorRangeScanReturnsResultError(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyOptions []oxia.RangeScanOption
	mockClient.On("RangeScan", "a", "z", emptyOptions).Return(scanResults(
		oxia.GetResult{Key: "a", Value: []byte("v")},
		oxia.GetResult{Err: errTest},
	))
	executor, out := newTestExecutor(mockClient)

	assert.ErrorIs(t, executor.Execute([]string{"range-scan", "a", "z"}), errTest)
	assert.Equal(t, "a\tv\n", out.String())
}

func TestExecutorRangeScanReturnsWriteError(t *testing.T) {
	for _, test := range []struct {
		name     string
		args     []string
		okWrites int
	}{
		{"hex-key", []string{"range-scan", "--hex"}, 0},
		{"record", []string{"range-scan"}, 0},
		{"version-separator", []string{"range-scan", "--include-version"}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			mockClient := common.NewMockClient()
			var emptyOptions []oxia.RangeScanOption
			mockClient.On("RangeScan", "", "", emptyOptions).
				Return(scanResults(oxia.GetResult{Key: "a", Value: []byte("v")}))
			writer := &failingWriter{okWrites: test.okWrites}
			executor := Executor{
				Ctx:    context.Background(),
				Client: func() (oxia.SyncClient, error) { return mockClient, nil },
				Out:    writer,
			}

			assert.ErrorIs(t, executor.Execute(test.args), errTest)
			assert.Equal(t, test.okWrites+1, writer.writes)
		})
	}
}

func TestExecutorRangeScanRejectsInvalidArgs(t *testing.T) {
	executor, out := newTestExecutor(common.NewMockClient())

	for _, args := range [][]string{
		{"range-scan", "--unknown"},
		{"range-scan", "--key-max", "z", "a"},
		{"range-scan", "a", "b", "c"},
	} {
		assert.EqualError(t, executor.Execute(args), "usage: range-scan [flags] [MIN_KEY [MAX_KEY]]", "args: %v", args)
	}
	assert.Empty(t, out.String())
}

func scanResults(results ...oxia.GetResult) chan oxia.GetResult {
	ch := make(chan oxia.GetResult, len(results))
	for _, result := range results {
		ch <- result
	}
	close(ch)
	return ch
}
