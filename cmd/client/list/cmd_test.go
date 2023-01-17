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

package list

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/cmd/client/common"
	"oxia/oxia"
	"testing"
)

func TestCobra(t *testing.T) {
	for _, test := range []struct {
		name                string
		args                []string
		expectedErr         error
		expectedKeyMinimums []string
		expectedKeyMaximums []string
	}{
		{"range", []string{"-n", "x", "-x", "y"}, nil, []string{"x"}, []string{"y"}},
		{"ranges", []string{"-n", "x1", "-x", "y1", "-n", "x2", "-x", "y2"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}},
		{"stdin", []string{}, nil, nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			Config = flags{}
			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeyMinimums, Config.keyMinimums)
				assert.Equal(t, test.expectedKeyMaximums, Config.keyMaximums)
				assert.True(t, invoked)
				return nil
			}
			err := Cmd.Execute()

			assert.ErrorIs(t, err, test.expectedErr)
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
		{"range",
			"",
			flags{
				keyMinimums: []string{"a"},
				keyMaximums: []string{"b"},
			},
			nil,
			[]common.Query{Query{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}}},
		{"ranges",
			"",
			flags{
				keyMinimums: []string{"a", "x"},
				keyMaximums: []string{"b", "y"},
			},
			nil,
			[]common.Query{Query{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}, Query{
				KeyMinimum: "x",
				KeyMaximum: "y",
			}}},
		{"stdin",
			"{\"key_minimum\":\"a\",\"key_maximum\":\"b\"}\n{\"key_minimum\":\"x\",\"key_maximum\":\"y\"}\n",
			flags{},
			nil,
			[]common.Query{Query{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}, Query{
				KeyMinimum: "x",
				KeyMaximum: "y",
			}}},
		{"range-no-min",
			"",
			flags{
				keyMaximums: []string{"b"},
			},
			ErrorExpectedRangeInconsistent,
			nil},
		{"range-no-max",
			"",
			flags{
				keyMaximums: []string{"b"},
			},
			ErrorExpectedRangeInconsistent,
			nil},
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
		expected Query
	}{
		{"key",
			"{\"key_minimum\":\"a\",\"key_maximum\":\"b\"}",
			Query{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := Query{}.Unmarshal([]byte(test.input))
			assert.Equal(t, test.expected, result)
			assert.Equal(t, err, nil)
		})
	}
}

func TestOutputMarshal(t *testing.T) {
	for _, test := range []struct {
		name     string
		output   Output
		expected string
	}{
		{"some",
			Output{
				Keys: []string{"a", "b"},
			},
			"{\"keys\":[\"a\",\"b\"]}",
		},
		{"none",
			Output{
				Keys: []string{},
			},
			"{\"keys\":[]}",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := json.Marshal(test.output)
			if err != nil {
				panic(err)
			}
			assert.Equal(t, test.expected, string(result))
		})
	}
}

func TestCall_Complete(t *testing.T) {
	tests := []struct {
		name         string
		clientResult oxia.ListResult
		expected     any
	}{
		{
			"keys",
			oxia.ListResult{
				Keys: []string{"a", "b"},
			}, Output{
				Keys: []string{"a", "b"},
			},
		},
		{
			"empty",
			oxia.ListResult{
				Keys: []string{},
			}, Output{
				Keys: []string{},
			},
		},
		{
			"error",
			oxia.ListResult{
				Err: errors.New("error"),
			}, common.OutputError{
				Err: "error",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(chan oxia.ListResult, 1)
			results <- test.clientResult
			call := Call{
				clientCall: results,
			}
			assert.Equalf(t, test.expected, call.Complete(), "Error")
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
