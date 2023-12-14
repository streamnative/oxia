// Copyright 2023 StreamNative, Inc.
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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/cmd/client/del"
	"github.com/streamnative/oxia/cmd/client/get"
	"github.com/streamnative/oxia/cmd/client/list"
	"github.com/streamnative/oxia/cmd/client/put"
	"github.com/streamnative/oxia/server"
)

func TestClientCmd(t *testing.T) {
	standaloneServer, err := server.NewStandalone(server.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	serviceAddress := fmt.Sprintf("localhost:%d", standaloneServer.RpcPort())

	stdin := bytes.NewBufferString("")
	stdout := bytes.NewBufferString("")
	stderr := bytes.NewBufferString("")
	Cmd.SetIn(stdin)
	Cmd.SetOut(stdout)
	Cmd.SetErr(stderr)

	for _, test := range []struct {
		name             string
		args             []string
		stdin            string
		expectedErr      error
		expectedStdOutRe string
		expectedStdErrRe string
	}{
		{"put", []string{"put", "-k", "k-put", "-v", "a"}, "", nil,
			"\\{\"version\":\\{\"version_id\":0,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"put-expected", []string{"put", "-k", "k-put", "-v", "c", "-e", "0"}, "", nil,
			"\\{\"version\":\\{\"version_id\":1,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":1}\\}",
			"^$",
		},
		{"put-unexpected-present", []string{"put", "-k", "k-put", "-v", "c", "-e", "0"}, "", nil,
			"\\{\"error\":\"unexpected version id\"\\}",
			"^$",
		},
		{"put-unexpected-not-present", []string{"put", "-k", "k-put-unp", "-v", "c", "-e", "9999"}, "", nil,
			"\\{\"error\":\"unexpected version id\"\\}",
			"^$",
		},
		{"put-multi", []string{"put", "--max-requests-per-batch", "1", "-k", "2a", "-v", "c", "-k", "2x", "-v", "y"}, "", nil,
			"\\{\"version\":\\{\"version_id\":4,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}\\n{\"version\":\\{\"version_id\":5,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"put-binary", []string{"put", "-k", "k-put-binary-ok", "-v", "aGVsbG8y", "-b"}, "", nil,
			"\\{\"version\":\\{\"version_id\":6,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"put-binary-fail", []string{"put", "-k", "k-put-binary-fail", "-v", "not-binary", "-b"}, "", nil,
			"\\{\"error\":\"binary flag was set but value is not valid base64\"\\}",
			"^$",
		},
		{"put-no-value", []string{"put", "-k", "k-put-np"}, "", put.ErrExpectedKeyValueInconsistent,
			".*",
			"Error: inconsistent flags; key and value flags must be in pairs",
		},
		{"put-no-key", []string{"put", "-v", "k-put-np"}, "", put.ErrExpectedKeyValueInconsistent,
			".*",
			"Error: inconsistent flags; key and value flags must be in pairs",
		},
		{"put-stdin", []string{"put"}, "{\"key\":\"3a\",\"value\":\"aGVsbG8y\",\"binary\":true}\n{\"key\":\"3x\",\"value\":\"aGVsbG8y\"}", nil,
			"\\{\"version\":\\{\"version_id\":7,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}\\n{\"version\":\\{\"version_id\":8,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"put-bad-binary-use", []string{"put", "-b"}, "", put.ErrIncorrectBinaryFlagUse,
			".*",
			"Error: binary flag was set when config is being sourced from stdin",
		},
		{"get", []string{"get", "-k", "k-put-binary-ok"}, "", nil,
			"\\{\"binary\":false,\"value\":\"hello2\",\"version\":\\{\"version_id\":6,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"get-binary", []string{"get", "-k", "k-put-binary-ok", "-b"}, "", nil,
			"\\{\"binary\":true,\"value\":\"aGVsbG8y\",\"version\":\\{\"version_id\":6,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"get-not-exist", []string{"get", "-k", "does-not-exist"}, "", nil,
			"\\{\"error\":\"key not found\"\\}",
			"^$",
		},
		{"get-multi", []string{"get", "-k", "2a", "-k", "2x"}, "", nil,
			"\\{\"binary\":false,\"value\":\"c\",\"version\":\\{\"version_id\":4,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}\\n{\"binary\":false,\"value\":\"y\",\"version\":\\{\"version_id\":5,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"get-stdin", []string{"get"}, "{\"key\":\"k-put-binary-ok\",\"binary\":true}\n{\"key\":\"2a\"}\n", nil,
			"\\{\"binary\":true,\"value\":\"aGVsbG8y\",\"version\":\\{\"version_id\":6,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}\\n{\"binary\":false,\"value\":\"c\",\"version\":\\{\"version_id\":4,\"created_timestamp\":\\d+,\"modified_timestamp\":\\d+\\,\"modifications_count\":0}\\}",
			"^$",
		},
		{"get-bad-binary-use", []string{"get", "-b"}, "", get.ErrIncorrectBinaryFlagUse,
			".*",
			"Error: binary flag was set when config is being sourced from stdin",
		},
		{"list-none", []string{"list", "--key-min", "XXX", "--key-max", "XXY"}, "", nil,
			"\\{\"keys\":\\[\\]\\}",
			"^$",
		},
		{"list-all", []string{"list", "--key-min", "a", "--key-max", "z"}, "", nil,
			"\\{\"keys\":\\[\"k-put\",\"k-put-binary-ok\"\\]\\}",
			"^$",
		},
		{"list-no-minimum", []string{"list", "--key-max", "XXY"}, "", list.ErrExpectedRangeInconsistent,
			".*",
			"Error: inconsistent flags; min and max flags must be in pairs",
		},
		{"list-no-maximum", []string{"list", "--key-min", "XXX"}, "", list.ErrExpectedRangeInconsistent,
			".*",
			"Error: inconsistent flags; min and max flags must be in pairs",
		},
		{"list-stdin", []string{"list"}, "{\"key_minimum\":\"j\",\"key_maximum\":\"l\"}\n{\"key_minimum\":\"a\",\"key_maximum\":\"b\"}\n", nil,
			"\\{\"keys\":\\[\"k-put\",\"k-put-binary-ok\"\\]\\}",
			"^$",
		},
		{"delete", []string{"delete", "-k", "k-put-binary-ok"}, "", nil,
			"\\{\\}",
			"^$",
		},
		{"delete-not-exist", []string{"delete", "-k", "does-not-exist"}, "", nil,
			"\\{\"error\":\"key not found\"\\}",
			"^$",
		},
		{"delete-unexpected-version", []string{"delete", "-k", "k-put", "-e", "9"}, "", nil,
			"\\{\"error\":\"unexpected version id\"\\}",
			"^$",
		},
		{"delete-expected-version", []string{"delete", "-k", "k-put", "-e", "1"}, "", nil,
			"\\{\\}",
			"^$",
		},
		{"delete-multi", []string{"delete", "-k", "2a", "-k", "2x"}, "", nil,
			"\\{\\}",
			"^$",
		},
		{"delete-multi-not-exist", []string{"delete", "-k", "2a", "-k", "2x"}, "", nil,
			"\\{\"error\":\"key not found\"\\}",
			"^$",
		},
		{"delete-multi-with-expected", []string{"delete", "-k", "2a", "-e", "0", "-k", "2x", "-e", "0"}, "", nil,
			"\\{\"error\":\"unexpected version id\"\\}\n\\{\"error\":\"unexpected version id\"\\}\n",
			"^$",
		},
		{"delete-range", []string{"delete", "--key-min", "q", "--key-max", "s"}, "", nil,
			"\\{\\}",
			"^$",
		},
		{"delete-range-with-expected", []string{"delete", "--key-min", "q", "--key-max", "s", "-e", "0"}, "", del.ErrExpectedVersionInconsistent,
			".*",
			"Error: inconsistent flags; zero or all keys must have an expected version",
		},
		{"delete-stdin", []string{"delete"}, "{\"key_minimum\":\"j\",\"key_maximum\":\"l\"}\n{\"key\":\"a\"}\n\n{\"key\":\"a\",\"expected_version\":0}\n", nil,
			"\\{\\}",
			"^$",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			put.Config.Reset()
			get.Config.Reset()
			list.Config.Reset()
			del.Config.Reset()

			stdin.WriteString(test.stdin)
			Cmd.SetArgs(append([]string{"-a", serviceAddress}, test.args...))
			err := Cmd.Execute()

			assert.Regexp(t, test.expectedStdOutRe, stdout.String())
			assert.Regexp(t, test.expectedStdErrRe, stderr.String())
			assert.ErrorIs(t, err, test.expectedErr)

			stdin.Reset()
			stdout.Reset()
			stderr.Reset()
		})
	}
	_ = standaloneServer.Close()
}
