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
			f = flags{}
			Cmd.SetArgs(test.args)
			invoked := false
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedKeyMinimums, f.keyMinimums)
				assert.Equal(t, test.expectedKeyMaximums, f.keyMaximums)
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
		expectedQueries []Query
	}{
		{"range",
			"",
			flags{
				keyMinimums: []string{"a"},
				keyMaximums: []string{"b"},
			},
			nil,
			[]Query{{
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
			[]Query{{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}, {
				KeyMinimum: "x",
				KeyMaximum: "y",
			}}},
		{"stdin",
			"{\"key_minimum\":\"a\",\"key_maximum\":\"b\"}\n{\"key_minimum\":\"x\",\"key_maximum\":\"y\"}\n",
			flags{},
			nil,
			[]Query{{
				KeyMinimum: "a",
				KeyMaximum: "b",
			}, {
				KeyMinimum: "x",
				KeyMaximum: "y",
			}}},
		{"range-no-min",
			"",
			flags{
				keyMaximums: []string{"b"},
			},
			ErrorExpectedRangeInconsistent,
			[]Query{}},
		{"range-no-max",
			"",
			flags{
				keyMaximums: []string{"b"},
			},
			ErrorExpectedRangeInconsistent,
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
		clientResult oxia.GetRangeResult
		expected     any
	}{
		{
			"keys",
			oxia.GetRangeResult{
				Keys: []string{"a", "b"},
			}, Output{
				Keys: []string{"a", "b"},
			},
		},
		{
			"empty",
			oxia.GetRangeResult{
				Keys: []string{},
			}, Output{
				Keys: []string{},
			},
		},
		{
			"error",
			oxia.GetRangeResult{
				Err: errors.New("error"),
			}, common.OutputError{
				Err: "error",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(chan oxia.GetRangeResult, 1)
			results <- test.clientResult
			call := Call{
				clientCall: results,
			}
			assert.Equalf(t, test.expected, call.Complete(), "Error")
		})
	}
}
