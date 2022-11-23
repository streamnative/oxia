package put

import (
	"bytes"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/cmd/client/common"
	"oxia/oxia"
	"testing"
)

func TestCobraFlags(t *testing.T) {
	for _, test := range []struct {
		name                   string
		args                   []string
		expectedErr            error
		expectedKeys           []string
		expectedPayloads       []string
		expectedVersions       []int64
		expectedBinaryPayloads bool
	}{
		{"entry", []string{"-k", "x", "-p", "y"}, nil, []string{"x"}, []string{"y"}, nil, false},
		{"entry-binary", []string{"-k", "x", "-p", "aGVsbG8=", "-b"}, nil, []string{"x"}, []string{"aGVsbG8="}, nil, true},
		{"entry-expected-version", []string{"-k", "x", "-p", "y", "-e", "1"}, nil, []string{"x"}, []string{"y"}, []int64{1}, false},
		{"entries", []string{"-k", "x1", "-p", "y1", "-k", "x2", "-p", "y2"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}, nil, false},
		{"entries-expected-version", []string{"-k", "x1", "-p", "y1", "-e", "1", "-k", "x2", "-p", "y2", "-e", "4"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}, []int64{1, 4}, false},
		{"stdin", []string{}, nil, nil, nil, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f = flags{}
			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeys, f.keys)
				assert.Equal(t, test.expectedVersions, f.expectedVersions)
				assert.Equal(t, test.expectedPayloads, f.payloads)
				assert.Equal(t, test.expectedBinaryPayloads, f.binaryPayloads)
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
		expectedQueries []Query
	}{
		{"entry",
			"",
			flags{
				keys:     []string{"x"},
				payloads: []string{"y"},
			},
			nil,
			[]Query{{
				Key:     "x",
				Payload: "y",
				Binary:  common.PtrBool(false),
			}}},
		{"entry-no-key",
			"",
			flags{
				payloads: []string{"y"},
			},
			ErrorExpectedKeyPayloadInconsistent,
			[]Query{}},
		{"entry-no-payload",
			"",
			flags{
				keys: []string{"x"},
			},
			ErrorExpectedKeyPayloadInconsistent,
			[]Query{}},
		{"entry-missing-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				payloads:         []string{"a", "b"},
				expectedVersions: []int64{1},
			},
			ErrorExpectedVersionInconsistent,
			[]Query{}},
		{"entry-binary",
			"",
			flags{
				keys:           []string{"x"},
				payloads:       []string{"aGVsbG8y"},
				binaryPayloads: true,
			},
			nil,
			[]Query{{
				Key:     "x",
				Payload: "aGVsbG8y",
				Binary:  common.PtrBool(true),
			}}},
		{"entry-expected-version",
			"",
			flags{
				keys:             []string{"x"},
				payloads:         []string{"y"},
				expectedVersions: []int64{1},
			},
			nil,
			[]Query{{
				Key:             "x",
				Payload:         "y",
				ExpectedVersion: common.PtrInt64(1),
				Binary:          common.PtrBool(false),
			}}},
		{"entries",
			"",
			flags{
				keys:           []string{"x", "y"},
				payloads:       []string{"a", "b"},
				binaryPayloads: true,
			},
			nil,
			[]Query{{
				Key:     "x",
				Payload: "a",
				Binary:  common.PtrBool(true),
			}, {
				Key:     "y",
				Payload: "b",
				Binary:  common.PtrBool(true),
			}}},
		{"entries-expected-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				payloads:         []string{"a", "b"},
				expectedVersions: []int64{1, 4},
			},
			nil,
			[]Query{{
				Key:             "x",
				Payload:         "a",
				ExpectedVersion: common.PtrInt64(1),
				Binary:          common.PtrBool(false),
			}, {
				Key:             "y",
				Payload:         "b",
				ExpectedVersion: common.PtrInt64(4),
				Binary:          common.PtrBool(false),
			}}},
		{"stdin",
			"{\"key\":\"a\",\"payload\":\"b\"}\n{\"key\":\"x\",\"payload\":\"y\"}\n",
			flags{},
			nil,
			[]Query{{
				Key:     "a",
				Payload: "b",
			}, {
				Key:     "x",
				Payload: "y",
			}}},
		{"stdin-binary",
			"{\"key\":\"a\",\"payload\":\"aGVsbG8y\",\"binary\":true}",
			flags{},
			nil,
			[]Query{{
				Key:     "a",
				Payload: "aGVsbG8y",
				Binary:  common.PtrBool(true),
			}}},
		{"stdin-binary-flag",
			"{\"key\":\"a\",\"payload\":\"aGVsbG8y\"}",
			flags{
				binaryPayloads: true,
			},
			ErrorIncorrectBinaryFlagUse,
			[]Query{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := bytes.NewBufferString(test.stdin)
			queries := make(chan common.Query, 10)
			results := make([]Query, 0)
			err := _exec(test.flags, in, queries)
			close(queries)
			for {
				query, ok := <-queries
				if !ok {
					break
				}
				results = append(results, query.(Query))
			}
			assert.Equal(t, test.expectedQueries, results)
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
			"{\"key\":\"a\",\"payload\":\"b\",\"expected_version\":1,\"binary\":true}",
			Query{
				Key:             "a",
				Payload:         "b",
				ExpectedVersion: common.PtrInt64(1),
				Binary:          common.PtrBool(true),
			}},
		{"key-binary",
			"{\"key\":\"a\",\"payload\":\"b\",\"binary\":true}",
			Query{
				Key:     "a",
				Payload: "b",
				Binary:  common.PtrBool(true),
			}},
		{"key-expected-version",
			"{\"key\":\"a\",\"payload\":\"b\",\"expected_version\":1}",
			Query{
				Key:             "a",
				Payload:         "b",
				ExpectedVersion: common.PtrInt64(1),
			}},
		{"key",
			"{\"key\":\"a\",\"payload\":\"b\"}",
			Query{
				Key:     "a",
				Payload: "b",
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
				Stat: common.OutputStat{
					Version:           1,
					CreatedTimestamp:  2,
					ModifiedTimestamp: 3,
				},
			},
			"{\"stat\":{\"version\":1,\"created_timestamp\":2,\"modified_timestamp\":3}}",
		},
		{"binary",
			Output{
				Stat: common.OutputStat{
					Version:           2,
					CreatedTimestamp:  4,
					ModifiedTimestamp: 6,
				},
			},
			"{\"stat\":{\"version\":2,\"created_timestamp\":4,\"modified_timestamp\":6}}",
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

func TestConvertPayload(t *testing.T) {
	for _, test := range []struct {
		name        string
		binary      bool
		payload     string
		expectedErr error
		expected    string
	}{
		{
			name:     "text",
			payload:  "hello",
			expected: "hello",
		},
		{
			name:     "binary",
			payload:  "aGVsbG8y",
			binary:   true,
			expected: "hello2",
		},
		{
			name:        "invalid-binary",
			payload:     "hello",
			binary:      true,
			expectedErr: ErrorBase64PayloadInvalid,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := convertPayload(test.binary, test.payload)
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
				Stat: oxia.Stat{
					Version:           1,
					CreatedTimestamp:  4,
					ModifiedTimestamp: 8,
				},
			}, Output{
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
			results := make(chan oxia.PutResult, 1)
			results <- test.clientResult
			call := Call{
				clientCall: results,
			}
			assert.Equalf(t, test.expected, call.Complete(), "Error")
		})
	}
}
