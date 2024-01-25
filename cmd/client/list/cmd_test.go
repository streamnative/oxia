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
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
)

func TestCobra(t *testing.T) {
	for _, test := range []struct {
		name           string
		args           []string
		expectedErr    error
		expectedKeyMin string
		expectedKeyMax string
	}{
		{"range", []string{"--key-min", "x", "--key-max", "y"}, nil, "x", "y"},
	} {
		t.Run(test.name, func(t *testing.T) {
			Config = flags{}
			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeyMin, Config.keyMin)
				assert.Equal(t, test.expectedKeyMax, Config.keyMax)
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
				keyMin: "a",
				keyMax: "b",
			},
			nil,
			[]common.Query{Query{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}}},
		{"range-no-min",
			"",
			flags{
				keyMax: "b",
			},
			nil,
			[]common.Query{Query{
				KeyMinimum: "",
				KeyMaximum: "b",
			}}},
		{"range-no-max",
			"",
			flags{
				keyMin: "a",
			},
			nil,
			[]common.Query{Query{
				KeyMinimum: "a",
				KeyMaximum: "",
			}}},
		{"range-no-limit",
			"",
			flags{},
			nil,
			[]common.Query{Query{
				KeyMinimum: "",
				KeyMaximum: "",
			}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := fakeQueryQueue{}
			err := _exec(test.flags, &queue)
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
			}, []string{"a", "b"},
		},
		{
			"empty",
			oxia.ListResult{
				Keys: []string{},
			}, []string{},
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
