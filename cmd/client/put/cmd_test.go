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

package put

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
)

func TestCobraFlags(t *testing.T) {
	for _, test := range []struct {
		name                 string
		args                 []string
		expectedErr          error
		expectedKeys         []string
		expectedValues       []string
		expectedVersions     []int64
		expectedBinaryValues bool
	}{
		{"entry", []string{"-k", "x", "-v", "y"}, nil, []string{"x"}, []string{"y"}, nil, false},
		{"entry-binary", []string{"-k", "x", "-v", "aGVsbG8=", "-b"}, nil, []string{"x"}, []string{"aGVsbG8="}, nil, true},
		{"entry-expected-version", []string{"-k", "x", "-v", "y", "-e", "1"}, nil, []string{"x"}, []string{"y"}, []int64{1}, false},
		{"entries", []string{"-k", "x1", "-v", "y1", "-k", "x2", "-v", "y2"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}, nil, false},
		{"entries-expected-version", []string{"-k", "x1", "-v", "y1", "-e", "1", "-k", "x2", "-v", "y2", "-e", "4"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}, []int64{1, 4}, false},
		{"stdin", []string{}, nil, nil, nil, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			Config = flags{}
			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeys, Config.keys)
				assert.Equal(t, test.expectedVersions, Config.expectedVersions)
				assert.Equal(t, test.expectedValues, Config.values)
				assert.Equal(t, test.expectedBinaryValues, Config.binaryValues)
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
		{"entry",
			"",
			flags{
				keys:   []string{"x"},
				values: []string{"y"},
			},
			nil,
			[]common.Query{Query{
				Key:    "x",
				Value:  "y",
				Binary: common.PtrBool(false),
			}}},
		{"entry-no-key",
			"",
			flags{
				values: []string{"y"},
			},
			ErrExpectedKeyValueInconsistent,
			nil},
		{"entry-no-value",
			"",
			flags{
				keys: []string{"x"},
			},
			ErrExpectedKeyValueInconsistent,
			nil},
		{"entry-missing-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				values:           []string{"a", "b"},
				expectedVersions: []int64{1},
			},
			ErrExpectedVersionInconsistent,
			nil},
		{"entry-binary",
			"",
			flags{
				keys:         []string{"x"},
				values:       []string{"aGVsbG8y"},
				binaryValues: true,
			},
			nil,
			[]common.Query{Query{
				Key:    "x",
				Value:  "aGVsbG8y",
				Binary: common.PtrBool(true),
			}}},
		{"entry-expected-version",
			"",
			flags{
				keys:             []string{"x"},
				values:           []string{"y"},
				expectedVersions: []int64{1},
			},
			nil,
			[]common.Query{Query{
				Key:             "x",
				Value:           "y",
				ExpectedVersion: common.PtrInt64(1),
				Binary:          common.PtrBool(false),
			}}},
		{"entries",
			"",
			flags{
				keys:         []string{"x", "y"},
				values:       []string{"a", "b"},
				binaryValues: true,
			},
			nil,
			[]common.Query{Query{
				Key:    "x",
				Value:  "a",
				Binary: common.PtrBool(true),
			}, Query{
				Key:    "y",
				Value:  "b",
				Binary: common.PtrBool(true),
			}}},
		{"entries-expected-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				values:           []string{"a", "b"},
				expectedVersions: []int64{1, 4},
			},
			nil,
			[]common.Query{Query{
				Key:             "x",
				Value:           "a",
				ExpectedVersion: common.PtrInt64(1),
				Binary:          common.PtrBool(false),
			}, Query{
				Key:             "y",
				Value:           "b",
				ExpectedVersion: common.PtrInt64(4),
				Binary:          common.PtrBool(false),
			}}},
		{"stdin",
			"{\"key\":\"a\",\"value\":\"b\"}\n{\"key\":\"x\",\"value\":\"y\"}\n",
			flags{},
			nil,
			[]common.Query{Query{
				Key:   "a",
				Value: "b",
			}, Query{
				Key:   "x",
				Value: "y",
			}}},
		{"stdin-binary",
			"{\"key\":\"a\",\"value\":\"aGVsbG8y\",\"binary\":true}",
			flags{},
			nil,
			[]common.Query{Query{
				Key:    "a",
				Value:  "aGVsbG8y",
				Binary: common.PtrBool(true),
			}}},
		{"stdin-binary-flag",
			"{\"key\":\"a\",\"value\":\"aGVsbG8y\"}",
			flags{
				binaryValues: true,
			},
			ErrIncorrectBinaryFlagUse,
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
		{"key-binary-expected-version",
			"{\"key\":\"a\",\"value\":\"b\",\"expected_version\":1,\"binary\":true}",
			Query{
				Key:             "a",
				Value:           "b",
				ExpectedVersion: common.PtrInt64(1),
				Binary:          common.PtrBool(true),
			}},
		{"key-binary",
			"{\"key\":\"a\",\"value\":\"b\",\"binary\":true}",
			Query{
				Key:    "a",
				Value:  "b",
				Binary: common.PtrBool(true),
			}},
		{"key-expected-version",
			"{\"key\":\"a\",\"value\":\"b\",\"expected_version\":1}",
			Query{
				Key:             "a",
				Value:           "b",
				ExpectedVersion: common.PtrInt64(1),
			}},
		{"key",
			"{\"key\":\"a\",\"value\":\"b\"}",
			Query{
				Key:   "a",
				Value: "b",
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
				Version: common.OutputVersion{
					VersionId:          1,
					CreatedTimestamp:   time.UnixMilli(2),
					ModifiedTimestamp:  time.UnixMilli(3),
					ModificationsCount: 0,
				},
			},
			"{\"version\":{\"version_id\":1,\"created_timestamp\":\"" + time.UnixMilli(2).Format(time.RFC3339Nano) +
				"\",\"modified_timestamp\":\"" + time.UnixMilli(3).Format(time.RFC3339Nano) + "\",\"modifications_count\":0,\"ephemeral\":false,\"client_identity\":\"\"}}",
		},
		{"binary",
			Output{
				Version: common.OutputVersion{
					VersionId:          2,
					CreatedTimestamp:   time.UnixMilli(4),
					ModifiedTimestamp:  time.UnixMilli(6),
					ModificationsCount: 2,
				},
			},
			"{\"version\":{\"version_id\":2,\"created_timestamp\":\"" + time.UnixMilli(4).Format(time.RFC3339Nano) +
				"\",\"modified_timestamp\":\"" + time.UnixMilli(6).Format(time.RFC3339Nano) + "\",\"modifications_count\":2,\"ephemeral\":false,\"client_identity\":\"\"}}",
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

func TestConvertBinaryValue(t *testing.T) {
	for _, test := range []struct {
		name        string
		value       string
		expectedErr error
		expected    string
	}{
		{
			name:     "binary",
			value:    "aGVsbG8y",
			expected: "hello2",
		},
		{
			name:        "invalid-binary",
			value:       "hello",
			expectedErr: ErrBase64ValueInvalid,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := convertFromBinaryValue(test.value)
			assert.Equal(t, test.expected, string(result))
			assert.ErrorIs(t, err, test.expectedErr)
		})
	}
}

func TestCall_Complete(t *testing.T) {
	tests := []struct {
		name         string
		clientResult oxia.PutResult
		expected     any
	}{
		{
			"error",
			oxia.PutResult{
				Err: errors.New("error"),
			}, common.OutputError{
				Err: "error",
			},
		},
		{
			"result",
			oxia.PutResult{
				Version: oxia.Version{
					VersionId:          1,
					CreatedTimestamp:   4,
					ModifiedTimestamp:  8,
					ModificationsCount: 1,
				},
			}, Output{
				Version: common.OutputVersion{
					VersionId:          1,
					CreatedTimestamp:   time.UnixMilli(4),
					ModifiedTimestamp:  time.UnixMilli(8),
					ModificationsCount: 1,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(chan oxia.PutResult, 1)
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
