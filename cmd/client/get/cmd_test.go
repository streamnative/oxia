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

package get

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/cmd/common/clientauth"
	"github.com/oxia-db/oxia/oxia"
)

func runCmd(cmd *cobra.Command, args string, stdin string) (string, error) {
	actual := new(bytes.Buffer)
	cmd.SetIn(bytes.NewBufferString(stdin))
	cmd.SetOut(actual)
	cmd.SetErr(actual)
	cmd.SetArgs(strings.Split(args, " "))
	err := cmd.Execute()
	Config.Reset()
	return strings.TrimSpace(actual.String()), err
}

func Test_exec(t *testing.T) {
	var emptyOptions []oxia.GetOption
	for _, test := range []struct {
		name               string
		args               string
		expectedParameters []any
		out                string
	}{
		{"key", "x", []any{"x", emptyOptions}, "value-x"},
		{"other-key", "y", []any{"y", emptyOptions}, "value-y"},
		{"partition-key", "y -p xyz", []any{"y", []oxia.GetOption{oxia.PartitionKey("xyz")}}, "value-y"},
		{"equal", "y -t equal", []any{"y", emptyOptions}, "value-y"},
		{"floor", "y -t floor", []any{"y", []oxia.GetOption{oxia.ComparisonFloor()}}, "value-y"},
		{"ceiling", "y -t ceiling", []any{"y", []oxia.GetOption{oxia.ComparisonCeiling()}}, "value-y"},
		{"lower", "y -t lower", []any{"y", []oxia.GetOption{oxia.ComparisonLower()}}, "value-y"},
		{"higher", "y -t higher", []any{"y", []oxia.GetOption{oxia.ComparisonHigher()}}, "value-y"},
		{"index", "y --index idx", []any{"y", []oxia.GetOption{oxia.UseIndex("idx")}}, "value-y"},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.MockedClient = common.NewMockClient()

			common.MockedClient.On("Get", test.expectedParameters...).
				Return(test.expectedParameters[0], []byte(test.out), oxia.Version{}, nil)
			out, err := runCmd(Cmd, test.args, "")
			assert.NoError(t, err)
			assert.Equal(t, test.out, out)

			common.MockedClient.AssertExpectations(t)
		})
	}
}

func TestExecRejectsInvalidComparisonType(t *testing.T) {
	useMockedClient(t)

	_, err := runCmd(Cmd, "y -t unknown", "")

	assert.EqualError(t, err, "invalid comparison type: unknown")
	common.MockedClient.AssertExpectations(t)
}

func TestExecWritesHexDump(t *testing.T) {
	useMockedClient(t)
	var emptyOptions []oxia.GetOption
	common.MockedClient.On("Get", "y", emptyOptions).Return("y", []byte("abc"), oxia.Version{}, nil)

	out, err := runCmd(Cmd, "y --hex", "")

	assert.NoError(t, err)
	assert.Equal(t, "00000000  61 62 63                                          |abc|", out)
	common.MockedClient.AssertExpectations(t)
}

func TestExecWritesVersion(t *testing.T) {
	useMockedClient(t)
	var emptyOptions []oxia.GetOption
	common.MockedClient.On("Get", "x", emptyOptions).Return("y", []byte("value-y"), oxia.Version{}, nil)

	out, err := runCmd(Cmd, "x -v", "")

	assert.NoError(t, err)
	value, version, found := strings.Cut(out, "\n---\n")
	assert.True(t, found)
	assert.Equal(t, "value-y", value)
	var outputVersion common.OutputVersion
	assert.NoError(t, json.Unmarshal([]byte(version), &outputVersion))
	assert.Equal(t, "y", outputVersion.Key)
	common.MockedClient.AssertExpectations(t)
}

func TestExecReturnsNewClientError(t *testing.T) {
	useConflictingClientAuth(t)

	_, err := runCmd(Cmd, "x", "")

	assert.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

func useMockedClient(t *testing.T) {
	t.Helper()
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })
}

// useConflictingClientAuth makes the creation of the client fail, with
// conflicting sources of the auth token.
func useConflictingClientAuth(t *testing.T) {
	t.Helper()
	mocked, auth := common.MockedClient, common.Config.Auth
	common.MockedClient = nil
	common.Config.Auth = clientauth.Config{Token: "token", TokenFile: "token-file"}
	t.Cleanup(func() { common.MockedClient, common.Config.Auth = mocked, auth })
}
