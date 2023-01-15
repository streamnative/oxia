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

package get

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
		name           string
		args           []string
		expectedErr    error
		expectedKeys   []string
		expectedBinary bool
	}{
		{"key", []string{"-k", "x"}, nil, []string{"x"}, false},
		{"key-binary", []string{"-k", "x", "-b"}, nil, []string{"x"}, true},
		{"keys", []string{"-k", "x", "-k", "y"}, nil, []string{"x", "y"}, false},
		{"keys-binary", []string{"-k", "x", "-k", "y", "-b"}, nil, []string{"x", "y"}, true},
		{"stdin", []string{}, nil, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			Config = flags{}
			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeys, Config.keys)
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
			[]common.Query{Query{
				Key: "x",
			}}},
		{"key-binary",
			"",
			flags{
				keys:           []string{"x"},
				binaryPayloads: true,
			},
			nil,
			[]common.Query{Query{
				Key:    "x",
				Binary: common.PtrBool(true),
			}}},
		{"keys",
			"",
			flags{
				keys: []string{"x", "y"},
			},
			nil,
			[]common.Query{Query{
				Key: "x",
			}, Query{
				Key: "y",
			}}},
		{"keys-binary",
			"",
			flags{
				keys:           []string{"x", "y"},
				binaryPayloads: true,
			},
			nil,
			[]common.Query{Query{
				Key:    "x",
				Binary: common.PtrBool(true),
			}, Query{
				Key:    "y",
				Binary: common.PtrBool(true),
			}}},
		{"stdin",
			"{\"key\":\"a\",\"binary\":true}",
			flags{},
			nil,
			[]common.Query{Query{
				Key:    "a",
				Binary: common.PtrBool(true),
			}}},
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
			"{\"key\":\"a\"}",
			Query{
				Key: "a",
			}},
		{"key-binary",
			"{\"key\":\"a\",\"binary\":true}",
			Query{
				Key:    "a",
				Binary: common.PtrBool(true),
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
		{"non-binary",
			Output{
				Binary:  common.PtrBool(false),
				Payload: "hello",
				Stat: common.OutputStat{
					Version:           1,
					CreatedTimestamp:  2,
					ModifiedTimestamp: 3,
				},
			},
			"{\"binary\":false,\"payload\":\"hello\",\"stat\":{\"version\":1,\"created_timestamp\":2,\"modified_timestamp\":3}}",
		},
		{"binary",
			Output{
				Binary:  common.PtrBool(true),
				Payload: "aGVsbG8y",
				Stat: common.OutputStat{
					Version:           2,
					CreatedTimestamp:  4,
					ModifiedTimestamp: 6,
				},
			},
			"{\"binary\":true,\"payload\":\"aGVsbG8y\",\"stat\":{\"version\":2,\"created_timestamp\":4,\"modified_timestamp\":6}}",
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
		clientResult oxia.GetResult
		binary       bool
		expected     any
	}{
		{
			"error",
			oxia.GetResult{
				Err: errors.New("error"),
			},
			false,
			common.OutputError{
				Err: "error",
			},
		},
		{
			"result",
			oxia.GetResult{
				Payload: []byte("hello"),
				Stat: oxia.Stat{
					Version:           1,
					CreatedTimestamp:  4,
					ModifiedTimestamp: 8,
				},
			},
			false,
			Output{
				Payload: "hello",
				Binary:  common.PtrBool(false),
				Stat: common.OutputStat{
					Version:           1,
					CreatedTimestamp:  4,
					ModifiedTimestamp: 8,
				},
			},
		},
		{
			"result-binary",
			oxia.GetResult{
				Payload: []byte("hello2"),
				Stat: oxia.Stat{
					Version:           1,
					CreatedTimestamp:  4,
					ModifiedTimestamp: 8,
				},
			},
			true,
			Output{
				Payload: "aGVsbG8y",
				Binary:  common.PtrBool(true),
				Stat: common.OutputStat{
					Version:           1,
					CreatedTimestamp:  4,
					ModifiedTimestamp: 8,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(chan oxia.GetResult, 1)
			results <- test.clientResult
			call := Call{
				binary:     test.binary,
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
