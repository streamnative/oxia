package delete

import (
	"bytes"
	"errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/cmd/client/common"
	"testing"
)

func TestCorba(t *testing.T) {
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
		{"range", []string{"-n", "x", "-x", "y"}, nil, nil, nil, []string{"x"}, []string{"y"}},
		{"ranges", []string{"-n", "x1", "-x", "y1", "-n", "x2", "-x", "y2"}, nil, nil, nil, []string{"x1", "x2"}, []string{"y1", "y2"}},
		{"stdin", []string{}, nil, nil, nil, nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			f = flags{}

			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeys, f.keys)
				assert.Equal(t, test.expectedVersions, f.expectedVersions)
				assert.Equal(t, test.expectedKeyMinimums, f.keyMinimums)
				assert.Equal(t, test.expectedKeyMaximums, f.keyMaximums)
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
		name                 string
		stdin                string
		flags                flags
		expectedErr          error
		expectedKeyQueries   []QueryByKey
		expectedRangeQueries []QueryByRange
	}{
		{"key",
			"",
			flags{
				keys: []string{"x"},
			},
			nil,
			[]QueryByKey{{
				Key: "x",
			}},
			nil},
		{"key-expected-version",
			"",
			flags{
				keys:             []string{"x"},
				expectedVersions: []int64{1},
			},
			nil,
			[]QueryByKey{{
				Key:             "x",
				ExpectedVersion: common.PtrInt64(1),
			}},
			nil},
		{"keys",
			"",
			flags{
				keys: []string{"x", "y"},
			},
			nil,
			[]QueryByKey{{
				Key: "x",
			}, {
				Key: "y",
			}},
			nil,
		},
		{"keys-expected-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				expectedVersions: []int64{1, 4},
			},
			nil,
			[]QueryByKey{{
				Key:             "x",
				ExpectedVersion: common.PtrInt64(1),
			}, {
				Key:             "y",
				ExpectedVersion: common.PtrInt64(4),
			}},
			nil,
		},
		{"missing-key",
			"",
			flags{
				expectedVersions: []int64{1},
			},
			ErrorExpectedVersionInconsistent,
			nil,
			nil},
		{"missing-version",
			"",
			flags{
				keys:             []string{"x", "y"},
				expectedVersions: []int64{1},
			},
			ErrorExpectedVersionInconsistent,
			nil,
			nil},
		{"range-no-max",
			"",
			flags{
				keyMinimums: []string{"a", "x"},
				keyMaximums: []string{"y"},
			},
			ErrorExpectedRangeInconsistent,
			nil,
			nil,
		},
		{"range",
			"",
			flags{
				keyMinimums: []string{"a", "x"},
				keyMaximums: []string{"b", "y"},
			},
			nil,
			nil,
			[]QueryByRange{{
				KeyMinimum: "a",
				KeyMaximum: "b",
			},
				{
					KeyMinimum: "x",
					KeyMaximum: "y",
				}},
		},
		{"range-no-min",
			"",
			flags{
				keyMinimums: []string{"a"},
				keyMaximums: []string{"b", "y"},
			},
			ErrorExpectedRangeInconsistent,
			nil,
			nil,
		},
		{"stdin",
			"{\"key\":\"x\"}\n{\"key\":\"y\",\"expected_version\":4}\n{\"key_minimum\":\"a\",\"key_maximum\":\"b\"}\n{\"key_minimum\":\"x\",\"key_maximum\":\"y\"}\n",
			flags{},
			nil,
			[]QueryByKey{{
				Key: "x",
			}, {
				Key:             "y",
				ExpectedVersion: common.PtrInt64(4),
			}},
			[]QueryByRange{{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}, {
				KeyMinimum: "x",
				KeyMaximum: "y",
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := bytes.NewBufferString(test.stdin)
			queries := make(chan common.Query, 10)
			keyResults := make([]QueryByKey, 0)
			rangeResults := make([]QueryByRange, 0)
			err := _exec(test.flags, in, queries)
			close(queries)
			for {
				query, ok := <-queries
				if !ok {
					break
				}
				switch q := query.(type) {
				case QueryByKey:
					keyResults = append(keyResults, q)
				case QueryByRange:
					rangeResults = append(rangeResults, q)
				}
			}
			if len(keyResults) < 1 {
				keyResults = nil
			}
			if len(rangeResults) < 1 {
				rangeResults = nil
			}
			assert.Equal(t, test.expectedKeyQueries, keyResults)
			assert.Equal(t, test.expectedRangeQueries, rangeResults)
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
			assert.Equalf(t, test.expected, call.Complete(), "Error")
		})
	}
}
