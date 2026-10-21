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

package admin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	admincommons "github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/oxia"
)

func TestExecutorShardSplitUsesDefaultNamespaceWithoutSplitPoint(t *testing.T) {
	adminClient := admincommons.NewMockAdminClient()
	adminClient.On("SplitShard", oxia.DefaultNamespace, int64(3), (*uint32)(nil)).Return(&oxia.SplitShardResult{
		LeftChildShardId:  4,
		RightChildShardId: 5,
	})
	executor, out := newTestExecutor(adminClient)

	require.NoError(t, executor.Execute([]string{"shard", "split", "--shard", "3"}))

	assert.Contains(t, out.String(), `"LeftChildShardId":4`)
	assert.Contains(t, out.String(), `"RightChildShardId":5`)
	adminClient.AssertExpectations(t)
}

func TestExecutorShardSplitReturnsSplitError(t *testing.T) {
	adminClient := admincommons.NewMockAdminClient()
	adminClient.On("SplitShard", "ns", int64(3), (*uint32)(nil)).Return(&oxia.SplitShardResult{Error: errTest})
	executor, out := newTestExecutor(adminClient)

	assert.ErrorIs(t, executor.Execute([]string{"shard", "split", "--namespace", "ns", "--shard", "3"}), errTest)
	assert.Empty(t, out.String())
	adminClient.AssertExpectations(t)
}

func TestExecutorShardSplitRejectsInvalidArgs(t *testing.T) {
	const usage = "usage: admin shard split --shard ID [--namespace NAME] [--split-point HASH]"
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unknown-flag", []string{"split", "--unknown"}, usage},
		{"no-shard", []string{"split"}, usage},
		{"extra-arg", []string{"split", "--shard", "1", "extra"}, usage},
		{"invalid-namespace", []string{"split", "--shard", "1", "--namespace", ""}, "namespace must not be empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			assert.EqualError(t, executor.Execute(append([]string{"shard"}, test.args...)), test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}
