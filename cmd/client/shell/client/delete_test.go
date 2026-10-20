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

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func TestExecutorDeleteReturnsClientError(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyOptions []oxia.DeleteOption
	mockClient.On("Delete", "k", emptyOptions).Return(oxia.ErrKeyNotFound)
	executor, out := newTestExecutor(mockClient)

	assert.ErrorIs(t, executor.Execute([]string{"delete", "k"}), oxia.ErrKeyNotFound)
	assert.Empty(t, out.String())
}

func TestExecutorDeleteRejectsInvalidArgs(t *testing.T) {
	executor, out := newTestExecutor(common.NewMockClient())

	for _, args := range [][]string{
		{"delete", "--unknown", "k"},
		{"delete"},
		{"delete", "a", "b"},
	} {
		assert.EqualError(t, executor.Execute(args), "usage: delete [flags] KEY", "args: %v", args)
	}
	assert.Empty(t, out.String())
}
