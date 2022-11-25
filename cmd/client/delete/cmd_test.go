package delete

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_exec(t *testing.T) {
	emptyKeys := []string{}
	emptyVersions := []int64{}
	emptyKeyMinimums := []string{}
	emptyKeyMaximums := []string{}
	for _, test := range []struct {
		name                string
		args                []string
		expectedErr         error
		expectedKeys        []string
		expectedVersions    []int64
		expectedKeyMinimums []string
		expectedKeyMaximums []string
	}{
		{"key", []string{"-k", "x"}, nil, []string{"x"}, emptyVersions, emptyKeyMinimums, emptyKeyMaximums},
		{"key-expected-version", []string{"-k", "x", "-e", "1"}, nil, []string{"x"}, []int64{1}, emptyKeyMinimums, emptyKeyMaximums},
		{"keys", []string{"-k", "x", "-k", "y"}, nil, []string{"x", "y"}, emptyVersions, emptyKeyMinimums, emptyKeyMaximums},
		{"keys-expected-version", []string{"-k", "x", "-e", "1", "-k", "y", "-e", "4"}, nil, []string{"x", "y"}, []int64{1, 4}, emptyKeyMinimums, emptyKeyMaximums},
		{"range", []string{"-n", "x", "-x", "y"}, nil, emptyKeys, emptyVersions, []string{"x"}, []string{"y"}},
		{"ranges", []string{"-n", "x1", "-x", "y1", "-n", "x2", "-x", "y2"}, nil, emptyKeys, emptyVersions, []string{"x1", "x2"}, []string{"y1", "y2"}},
		{"stdin", []string{}, nil, emptyKeys, emptyVersions, emptyKeyMinimums, emptyKeyMaximums},
		{"missing-key", []string{"-e", "1"}, ErrorExpectedVersionInconsistent, emptyKeys, []int64{1}, emptyKeyMinimums, emptyKeyMaximums},
		{"missing-version", []string{"-k", "x", "-k", "y", "-e", "4"}, ErrorExpectedVersionInconsistent, []string{"x", "y"}, []int64{4}, emptyKeyMinimums, emptyKeyMaximums},
		{"range-no-min", []string{"-x", "y"}, ErrorExpectedRangeInconsistent, emptyKeys, emptyVersions, emptyKeyMinimums, []string{"y"}},
		{"range-no-max", []string{"-n", "x"}, ErrorExpectedRangeInconsistent, emptyKeys, emptyVersions, []string{"x"}, emptyKeyMaximums},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys = emptyKeys
			expectedVersions = emptyVersions
			keyMinimums = emptyKeyMinimums
			keyMaximums = emptyKeyMaximums

			buf := new(bytes.Buffer)
			Cmd.SetArgs(test.args)
			Cmd.SetOut(buf)
			Cmd.SetErr(buf)
			err := Cmd.Execute()
			assert.Equal(t, test.expectedKeys, keys)
			assert.Equal(t, test.expectedVersions, expectedVersions)
			assert.Equal(t, test.expectedKeyMinimums, keyMinimums)
			assert.Equal(t, test.expectedKeyMaximums, keyMaximums)
			assert.ErrorIs(t, err, test.expectedErr)

			buf.Reset()
		})
	}
}
