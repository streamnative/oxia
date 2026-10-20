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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func TestExecutorPutPassesVersionAndSequenceOptions(t *testing.T) {
	mockClient := common.NewMockClient()
	options := []oxia.PutOption{
		oxia.ExpectedVersionId(3),
		oxia.SequenceKeysDeltas(1, 2),
	}
	mockClient.On("Put", "k", []byte("v"), options).Return("k-0001", oxia.Version{}, nil)
	executor, out := newTestExecutor(mockClient)

	require.NoError(t, executor.Execute([]string{"put", "-e", "3", "-d", "1,2", "k", "v"}))

	assert.Contains(t, out.String(), `"key":"k-0001"`)
	mockClient.AssertExpectations(t)
}

func TestExecutorPutReturnsClientError(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyOptions []oxia.PutOption
	mockClient.On("Put", "k", []byte("v"), emptyOptions).Return("", oxia.Version{}, oxia.ErrUnexpectedVersionId)
	executor, out := newTestExecutor(mockClient)

	assert.ErrorIs(t, executor.Execute([]string{"put", "k", "v"}), oxia.ErrUnexpectedVersionId)
	assert.Empty(t, out.String())
}

func TestExecutorPutRejectsInvalidArgs(t *testing.T) {
	executor, _ := newTestExecutor(common.NewMockClient())

	assert.EqualError(t, executor.Execute([]string{"put", "k"}), "usage: put [flags] KEY VALUE")
}

func TestParsePutCommandRejectsInvalidArgs(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unknown-flag", []string{"put", "--unknown", "k", "v"}, "usage: put [flags] KEY VALUE"},
		{"no-value", []string{"put", "k"}, "usage: put [flags] KEY VALUE"},
		{"version-and-create-only", []string{"put", "-e", "1", "--create-only", "k", "v"},
			"expected-version and create-only cannot both be set"},
		{"empty-delta", []string{"put", "-d", "1,,2", "k", "v"}, `invalid sequence key delta ""`},
		{"non-numeric-delta", []string{"put", "-d", "x", "k", "v"}, `invalid sequence key delta "x"`},
		{"index-without-separator", []string{"put", "--index", "idx", "k", "v"},
			"secondary index must be in NAME=KEY format"},
		{"index-without-name", []string{"put", "--index", "=key", "k", "v"},
			"secondary index must be in NAME=KEY format"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := parsePutCommand(test.args)

			assert.EqualError(t, err, test.error)
		})
	}
}

func TestParseUint64CSVParsesDeltas(t *testing.T) {
	deltas, err := parseUint64CSV("1,20,300")

	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 20, 300}, deltas)
}
