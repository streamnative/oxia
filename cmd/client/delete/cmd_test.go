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

package delete

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/cmd/client/common"
)

func TestCobra(t *testing.T) {
	for _, test := range []struct {
		name                string
		args                []string
		expectedErr         error
		expectedKeys        []string
		expectedVersions    []int64
		expectedKeyMinimums []string
		expectedKeyMaximums []string
	}{
		{"key", []string{"-k", "x"}, nil, []string{"x"}, nil, nil, nil},
		{"key-expected-version", []string{"-k", "x", "-e", "1"}, nil, []string{"x"}, []int64{1}, nil, nil},
		{"keys", []string{"-k", "x", "-k", "y"}, nil, []string{"x", "y"}, nil, nil, nil},
		{"keys-expected-version", []string{"-k", "x", "-e", "1", "-k", "y", "-e", "4"}, nil, []string{"x", "y"}, []int64{1, 4}, nil, nil},
		{"range", []string{"--key-min", "x", "--key-max", "y"}, nil, nil, nil, []string{"x"}, []string{"y"}},
		{"ranges", []string{"--key-min", "x1", "--key-max", "y1", "--key-min", "x2", "--key-max", "y2"}, nil, nil, nil, []string{"x1", "x2"}, []string{"y1", "y2"}},
		{"stdin", []string{}, nil, nil, nil, nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			Config = flags{}

			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeys, Config.keys)
				assert.Equal(t, test.expectedVersions, Config.expectedVersions)
				assert.Equal(t, test.expectedKeyMinimums, Config.keyMinimums)
				assert.Equal(t, test.expectedKeyMaximums, Config.keyMaximums)
				return nil
			}
			err := Cmd.Execute()
			assert.ErrorIs(t, err, test.expectedErr)
			assert.True(t, invoked)
		})
	}
}

func Test_exec(t *testing.T) {
	for _, test := range []struct {
		name            string
		stdin           string
		flags           flags
		expectedErr     error
		expectedQueries []common.Query
	}{
		{"key",
			"",
			flags{
				keys: []string{"x"},
			},
			nil,
			[]common.Query{QueryByKey{
				Key: "x",
			}}},
		{"key-expected-version",
			"",
			flags{
				keys:             []string{"x"},
				expectedVersions: []int64{1},
			},
			nil,
			[]common.Query{QueryByKey{
				Key:             "x",
				ExpectedVersion: common.PtrInt64(1),
			}}},
		{"keys",
			"",
			flags{
				keys: []string{"x", "y"},
			},
			nil,
			[]common.Query{QueryByKey{
				Key: "x",
			}, QueryByKey{
				Key: "y",
			}}},
		{"keys-expected-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				expectedVersions: []int64{1, 4},
			},
			nil,
			[]common.Query{QueryByKey{
				Key:             "x",
				ExpectedVersion: common.PtrInt64(1),
			}, QueryByKey{
				Key:             "y",
				ExpectedVersion: common.PtrInt64(4),
			}}},
		{"missing-key",
			"",
			flags{
				expectedVersions: []int64{1},
			},
			ErrExpectedVersionInconsistent,
			nil},
		{"missing-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				expectedVersions: []int64{1},
			},
			ErrExpectedVersionInconsistent,
			nil},
		{"range-no-max",
			"",
			flags{
				keyMinimums: []string{"a", "x"},
				keyMaximums: []string{"y"},
			},
			ErrExpectedRangeInconsistent,
			nil,
		},
		{"range",
			"",
			flags{
				keyMinimums: []string{"a", "x"},
				keyMaximums: []string{"b", "y"},
			},
			nil,
			[]common.Query{QueryByRange{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}, QueryByRange{
				KeyMinimum: "x",
				KeyMaximum: "y",
			}}},
		{"range-no-min",
			"",
			flags{
				keyMinimums: []string{"a"},
				keyMaximums: []string{"b", "y"},
			},
			ErrExpectedRangeInconsistent,
			nil,
		},
		{"stdin",
			"{\"key\":\"x\"}\n{\"key\":\"y\",\"expected_version\":4}\n{\"key_minimum\":\"a\",\"key_maximum\":\"b\"}\n{\"key_minimum\":\"x\",\"key_maximum\":\"y\"}\n",
			flags{},
			nil,
			[]common.Query{QueryByKey{
				Key: "x",
			}, QueryByKey{
				Key:             "y",
				ExpectedVersion: common.PtrInt64(4),
			}, QueryByRange{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}, QueryByRange{
				KeyMinimum: "x",
				KeyMaximum: "y",
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := bytes.NewBufferString(test.stdin)
			queue := fakeQueryQueue{}
			err := _exec(test.flags, in, &queue)
			assert.Equal(t, test.expectedQueries, queue.queries)
			assert.ErrorIs(t, err, test.expectedErr)
		})
	}
}

func TestInputUnmarshal(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		expected common.Query
	}{
		{"key",
			"{\"key\":\"a\"}",
			QueryByKey{
				Key: "a",
			}},
		{"key-expected-version",
			"{\"key\":\"a\",\"expected_version\":1}",
			QueryByKey{
				Key:             "a",
				ExpectedVersion: common.PtrInt64(1),
			}},
		{"range",
			"{\"key_minimum\":\"a\",\"key_maximum\":\"b\"}",
			QueryByRange{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := QueryInput{}.Unmarshal([]byte(test.input))
			assert.Equal(t, test.expected, result)
			assert.Equal(t, err, nil)
		})
	}
}

// Output marshalling is tested in common/model

func TestCall_Complete(t *testing.T) {
	tests := []struct {
		name         string
		clientResult error
		expected     any
	}{
		{
			"error",
			errors.New("error"),
			common.OutputError{
				Err: "error",
			},
		},
		{
			"no-error",
			nil,
			common.OutputError{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(chan error, 1)
			results <- test.clientResult
			call := Call{
				clientCall: results,
			}
			assert.Equalf(t, test.expected, <-call.Complete(), "Error")
		})
	}
}

type fakeQueryQueue struct {
	queries []common.Query
}

func (q *fakeQueryQueue) Add(query common.Query) {
	if q.queries == nil {
		q.queries = []common.Query{}
	}
	q.queries = append(q.queries, query)
}
