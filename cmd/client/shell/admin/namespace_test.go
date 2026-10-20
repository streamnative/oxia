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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	admincommons "github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func TestExecutorNamespaceReturnsAdminClientError(t *testing.T) {
	adminClient := admincommons.NewMockAdminClient()
	adminClient.On("ListNamespaces").Return([]*proto.NamespaceView(nil), errTest)
	adminClient.On("GetNamespace", "ns").Return((*proto.NamespaceView)(nil), errTest)
	adminClient.On("CreateNamespace", mock.Anything).Return((*proto.Namespace)(nil), errTest)
	adminClient.On("PatchNamespace", mock.Anything).Return((*proto.Namespace)(nil), errTest)
	adminClient.On("DeleteNamespace", "ns").Return((*proto.Namespace)(nil), errTest)
	executor, out := newTestExecutor(adminClient)

	for _, args := range [][]string{
		{"namespace", "list"},
		{"namespace", "get", "ns"},
		{"namespace", "create", "--initial-shards", "1", "--replication-factor", "1", "ns"},
		{"namespace", "patch", "--notifications=false", "ns"},
		{"namespace", "del", "ns"},
	} {
		assert.ErrorIs(t, executor.Execute(args), errTest, "args: %v", args)
	}
	assert.Empty(t, out.String())
	adminClient.AssertExpectations(t)
}

func TestExecutorNamespaceCreateRejectsInvalidArgs(t *testing.T) {
	const usage = "usage: admin namespace create [--output FORMAT] --initial-shards COUNT " +
		"--replication-factor FACTOR [--notifications BOOL] [--key-sorting TYPE] NAMESPACE"
	required := []string{"--initial-shards", "1", "--replication-factor", "1"}
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unknown-flag", []string{"--unknown", "ns"}, usage},
		{"no-name", required, usage},
		{"no-initial-shards", []string{"--replication-factor", "1", "ns"},
			"namespace initial shard count is required"},
		{"no-replication-factor", []string{"--initial-shards", "1", "ns"},
			"namespace replication factor is required"},
		{"zero-initial-shards", []string{"--initial-shards", "0", "--replication-factor", "1", "ns"},
			"namespace initial shard count must be greater than 0"},
		{"zero-replication-factor", []string{"--initial-shards", "1", "--replication-factor", "0", "ns"},
			"namespace replication factor must be greater than 0"},
		{"invalid-name", append(append([]string{}, required...), "a/b"),
			`namespace "a/b" contains invalid path separator`},
		{"invalid-key-sorting", append(append([]string{}, required...), "--key-sorting", "random", "ns"),
			`must be one of "natural" or "hierarchical"`},
		{"unknown-key-sorting", append(append([]string{}, required...), "--key-sorting", "unknown", "ns"),
			`key sorting must be one of "natural" or "hierarchical"`},
		{"invalid-output", append(append([]string{}, required...), "-o", "name", "ns"),
			`unsupported output format "name", expected one of: json, yaml, table`},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			err := executor.Execute(append([]string{"namespace", "create"}, test.args...))

			assert.EqualError(t, err, test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}

func TestExecutorNamespacePatchSetsNotifications(t *testing.T) {
	adminClient := admincommons.NewMockAdminClient()
	adminClient.On("PatchNamespace", mock.MatchedBy(func(namespace *proto.Namespace) bool {
		return namespace.GetName() == "ns" &&
			namespace.GetReplicationFactor() == 0 &&
			namespace.NotificationsEnabled != nil &&
			!namespace.GetNotificationsEnabled()
	})).Return(&proto.Namespace{Name: "ns", ReplicationFactor: 3}, nil)
	executor, out := newTestExecutor(adminClient)

	require.NoError(t, executor.Execute([]string{"namespace", "patch", "--notifications=false", "-o", "json", "ns"}))

	var namespace proto.Namespace
	require.NoError(t, json.Unmarshal(out.Bytes(), &namespace))
	assert.Equal(t, "ns", namespace.GetName())
	adminClient.AssertExpectations(t)
}

func TestExecutorNamespacePatchRejectsInvalidArgs(t *testing.T) {
	const usage = "usage: admin namespace patch [--output FORMAT] [--replication-factor FACTOR] " +
		"[--notifications BOOL] NAMESPACE"
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unknown-flag", []string{"--unknown", "ns"}, usage},
		{"no-name", []string{"--replication-factor", "1"}, usage},
		{"no-fields", []string{"ns"}, "must specify at least one field to patch"},
		{"zero-replication-factor", []string{"--replication-factor", "0", "ns"},
			"namespace replication factor must be greater than 0"},
		{"invalid-name", []string{"--replication-factor", "1", "a..b"},
			`namespace "a..b" contains invalid path traversal sequence`},
		{"invalid-output", []string{"--replication-factor", "1", "-o", "name", "ns"},
			`unsupported output format "name", expected one of: json, yaml, table`},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			err := executor.Execute(append([]string{"namespace", "patch"}, test.args...))

			assert.EqualError(t, err, test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}
