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

package put

import (
	"bytes"
	"errors"
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

func TestPut_exec(t *testing.T) {
	var emptyOptions []oxia.PutOption
	for _, test := range []struct {
		name               string
		args               string
		stdin              string
		expectedParameters []any
	}{
		{"entry", "x y", "", []any{"x", []byte("y"), emptyOptions}},
		{"entry-expected-version", "x y -e 5", "", []any{"x", []byte("y"), []oxia.PutOption{oxia.ExpectedVersionId(5)}}},
		{"stdin", "x -c -e 5", "my-value", []any{"x", []byte("my-value"), []oxia.PutOption{oxia.ExpectedVersionId(5)}}},
		{"partition-key", "x y -p abc", "", []any{"x", []byte("y"), []oxia.PutOption{oxia.PartitionKey("abc")}}},
		{"sequence-keys", "x y -p abc -d 1,2,3", "", []any{"x", []byte("y"),
			[]oxia.PutOption{oxia.PartitionKey("abc"), oxia.SequenceKeysDeltas(1, 2, 3)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.MockedClient = common.NewMockClient()

			common.MockedClient.On("Put", test.expectedParameters...).Return(test.expectedParameters[0], oxia.Version{}, nil)
			_, _ = runCmd(Cmd, test.args, test.stdin)

			common.MockedClient.AssertExpectations(t)
		})
	}
}

func TestExecReturnsNewClientError(t *testing.T) {
	useConflictingClientAuth(t)

	_, err := runCmd(Cmd, "x y", "")

	assert.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

func TestExecRejectsValueArgumentWithStdIn(t *testing.T) {
	useMockedClient(t)

	_, err := runCmd(Cmd, "x y -c", "z")

	assert.EqualError(t, err, "the value can either be provided as argument or read from std-in")
	common.MockedClient.AssertExpectations(t)
}

func TestExecReturnsStdInReadError(t *testing.T) {
	useMockedClient(t)
	Cmd.SetIn(&failingReader{})
	Cmd.SetOut(new(bytes.Buffer))
	Cmd.SetErr(new(bytes.Buffer))
	Cmd.SetArgs([]string{"x", "-c"})

	err := Cmd.Execute()
	Config.Reset()

	assert.ErrorIs(t, err, errRead)
	common.MockedClient.AssertExpectations(t)
}

func useMockedClient(t *testing.T) {
	t.Helper()
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })
}

var errRead = errors.New("read failed")

type failingReader struct{}

func (*failingReader) Read([]byte) (int, error) {
	return 0, errRead
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
