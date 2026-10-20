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

package split

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/cmd/common/clientauth"
	"github.com/oxia-db/oxia/oxia"
)

func TestExecSplitsAtMidpointWithoutSplitPoint(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("SplitShard", "default", int64(3), (*uint32)(nil)).
		Return(&oxia.SplitShardResult{LeftChildShardId: 4, RightChildShardId: 5})

	out, err := runSplit(t, "--shard", "3")

	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	assert.EqualValues(t, 4, result["LeftChildShardId"])
	assert.EqualValues(t, 5, result["RightChildShardId"])
	commons.MockedAdminClient.AssertExpectations(t)
}

func TestExecReturnsAdminClientError(t *testing.T) {
	commons.MockedAdminClient = nil
	commons.AdminConfig.Auth = clientauth.Config{Token: "token", TokenFile: "token-file"}
	t.Cleanup(func() { commons.AdminConfig.Auth = clientauth.Config{} })

	_, err := runSplit(t, "--shard", "3")

	require.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

func TestExecReturnsSplitError(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("SplitShard", "tenant", int64(3), mock.Anything).
		Return(&oxia.SplitShardResult{Error: errors.New("shard is splitting")})

	out, err := runSplit(t, "--namespace", "tenant", "--shard", "3")

	require.EqualError(t, err, "shard is splitting")
	assert.NotContains(t, out.String(), "LeftChildShardId")
	commons.MockedAdminClient.AssertExpectations(t)
}

func TestExecReturnsErrorWithoutSplitResult(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	// Without a result to return, the mock fails the split
	commons.MockedAdminClient.On("SplitShard", "default", int64(3), mock.Anything).Return(nil)

	_, err := runSplit(t, "--shard", "3")

	require.EqualError(t, err, "split shard failed")
	commons.MockedAdminClient.AssertExpectations(t)
}

func runSplit(t *testing.T, args ...string) (*bytes.Buffer, error) {
	t.Helper()
	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	cmd.Flags().AddFlagSet(Cmd.Flags())
	t.Cleanup(func() {
		namespace = "default"
		shardId = -1
		splitPoint = 0
	})
	out := new(bytes.Buffer)
	cmd.SetOut(out)
	cmd.SetErr(new(bytes.Buffer))
	cmd.SetArgs(args)
	return out, cmd.Execute()
}
