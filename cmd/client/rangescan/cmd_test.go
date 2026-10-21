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

package rangescan

import (
	"bytes"
	"encoding/json"
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

func TestRangeScan_exec(t *testing.T) {
	var emptyOptions []oxia.RangeScanOption
	for _, test := range []struct {
		name               string
		args               string
		expectedParameters []any
		results            []string
	}{
		{"range", "--key-min a --key-max c", []any{"a", "c", emptyOptions}, []string{"a", "b"}},
		{"short", "-s a -e c", []any{"a", "c", emptyOptions}, []string{"a", "b"}},
		{"range-no-min", "--key-max c", []any{"", "c", emptyOptions}, []string{"a", "b"}},
		{"range-no-max", "--key-min a", []any{"a", "", emptyOptions}, []string{"a", "b", "c"}},
		{"partition-key", "-s a -e c -p xyz", []any{"a", "c", []oxia.RangeScanOption{oxia.PartitionKey("xyz")}}, []string{"a", "b"}},
		{"index", "-s a -e c --index idx", []any{"a", "c", []oxia.RangeScanOption{oxia.UseIndex("idx")}}, []string{"a"}},
		{"internal-keys", "-s a -e c --internal-keys", []any{"a", "c",
			[]oxia.RangeScanOption{oxia.ShowInternalKeys(true)}}, []string{"a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.MockedClient = common.NewMockClient()

			ch := make(chan oxia.GetResult, 100)
			for _, res := range test.results {
				ch <- oxia.GetResult{
					Key:     res,
					Value:   []byte(res),
					Version: oxia.Version{},
					Err:     nil,
				}
			}
			close(ch)

			common.MockedClient.On("RangeScan", test.expectedParameters...).Return(ch)
			out, err := runCmd(Cmd, test.args, "")
			assert.NoError(t, err)

			expectedOut := strings.Join(test.results, "\n"+lineSeparator)
			assert.Equal(t, expectedOut, out)

			common.MockedClient.AssertExpectations(t)
		})
	}
}

func TestExecReturnsResultError(t *testing.T) {
	useMockedClient(t)
	var emptyOptions []oxia.RangeScanOption
	common.MockedClient.On("RangeScan", "a", "c", emptyOptions).Return(results(
		oxia.GetResult{Key: "a", Value: []byte("a")},
		oxia.GetResult{Err: errScan},
	))

	out, err := runCmd(Cmd, "-s a -e c", "")

	assert.ErrorIs(t, err, errScan)
	assert.True(t, strings.HasPrefix(out, "a\n"))
	common.MockedClient.AssertExpectations(t)
}

func TestExecWritesHexDump(t *testing.T) {
	useMockedClient(t)
	var emptyOptions []oxia.RangeScanOption
	common.MockedClient.On("RangeScan", "a", "c", emptyOptions).Return(results(
		oxia.GetResult{Key: "a", Value: []byte("abc")},
	))

	out, err := runCmd(Cmd, "-s a -e c --hex", "")

	assert.NoError(t, err)
	assert.Equal(t, "00000000  61 62 63                                          |abc|", out)
	common.MockedClient.AssertExpectations(t)
}

func TestExecWritesVersion(t *testing.T) {
	useMockedClient(t)
	var emptyOptions []oxia.RangeScanOption
	common.MockedClient.On("RangeScan", "a", "c", emptyOptions).Return(results(
		oxia.GetResult{Key: "a", Value: []byte("value-a"), Version: oxia.Version{VersionId: 3}},
	))

	out, err := runCmd(Cmd, "-s a -e c -v", "")

	assert.NoError(t, err)
	value, version, found := strings.Cut(out, "\n---\n")
	assert.True(t, found)
	assert.Equal(t, "value-a", value)
	var outputVersion common.OutputVersion
	assert.NoError(t, json.Unmarshal([]byte(version), &outputVersion))
	assert.Equal(t, "a", outputVersion.Key)
	assert.EqualValues(t, 3, outputVersion.VersionId)
	common.MockedClient.AssertExpectations(t)
}

func TestExecReturnsNewClientError(t *testing.T) {
	useConflictingClientAuth(t)

	_, err := runCmd(Cmd, "-s a -e b", "")

	assert.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

var errScan = errors.New("scan failed")

func useMockedClient(t *testing.T) {
	t.Helper()
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })
}

// results returns a closed channel with the given results of a range scan.
func results(getResults ...oxia.GetResult) chan oxia.GetResult {
	ch := make(chan oxia.GetResult, len(getResults))
	for _, result := range getResults {
		ch <- result
	}
	close(ch)
	return ch
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
