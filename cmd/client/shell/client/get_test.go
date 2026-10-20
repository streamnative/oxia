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

func TestExecutorGetWritesHexDump(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyOptions []oxia.GetOption
	mockClient.On("Get", "k", emptyOptions).Return("k", []byte("value"), oxia.Version{}, nil)
	executor, out := newTestExecutor(mockClient)

	require.NoError(t, executor.Execute([]string{"get", "--hex", "k"}))

	assert.Equal(t, hex.Dump([]byte("value")), out.String())
	mockClient.AssertExpectations(t)
}

func TestExecutorGetReturnsClientError(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyOptions []oxia.GetOption
	mockClient.On("Get", "k", emptyOptions).Return("", []byte(nil), oxia.Version{}, oxia.ErrKeyNotFound)
	executor, out := newTestExecutor(mockClient)

	assert.ErrorIs(t, executor.Execute([]string{"get", "k"}), oxia.ErrKeyNotFound)
	assert.Empty(t, out.String())
}

func TestExecutorGetReturnsVersionSeparatorWriteError(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyOptions []oxia.GetOption
	mockClient.On("Get", "k", emptyOptions).Return("k", []byte("value"), oxia.Version{}, nil)
	writer := &failingWriter{okWrites: 2}
	executor := Executor{
		Ctx:    context.Background(),
		Client: func() (oxia.SyncClient, error) { return mockClient, nil },
		Out:    writer,
	}

	assert.ErrorIs(t, executor.Execute([]string{"get", "--include-version", "k"}), errTest)
	assert.Equal(t, 3, writer.writes)
}

func TestExecutorGetRejectsInvalidArgs(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unknown-flag", []string{"get", "--unknown", "k"}, "usage: get [flags] KEY"},
		{"no-key", []string{"get"}, "usage: get [flags] KEY"},
		{"two-keys", []string{"get", "a", "b"}, "usage: get [flags] KEY"},
		{"comparison-type", []string{"get", "-t", "around", "k"}, "invalid comparison type: around"},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor, out := newTestExecutor(common.NewMockClient())

			assert.EqualError(t, executor.Execute(test.args), test.error)
			assert.Empty(t, out.String())
		})
	}
}

func TestParseGetCommandParsesComparisonTypes(t *testing.T) {
	for _, test := range []struct {
		comparisonType string
		expected       []oxia.GetOption
	}{
		{"equal", nil},
		{"floor", []oxia.GetOption{oxia.ComparisonFloor()}},
		{"ceiling", []oxia.GetOption{oxia.ComparisonCeiling()}},
		{"lower", []oxia.GetOption{oxia.ComparisonLower()}},
		{"higher", []oxia.GetOption{oxia.ComparisonHigher()}},
	} {
		t.Run(test.comparisonType, func(t *testing.T) {
			options, key, err := parseGetCommand([]string{"get", "--comparison-type", test.comparisonType, "k"})

			require.NoError(t, err)
			assert.Equal(t, "k", key)
			assert.Equal(t, test.expected, options.clientOptions)
		})
	}
}
