package list

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_exec(t *testing.T) {
	emptyKeyMinimums := []string{}
	emptyKeyMaximums := []string{}
	for _, test := range []struct {
		name                string
		args                []string
		expectedErr         error
		expectedKeyMinimums []string
		expectedKeyMaximums []string
	}{
		{"range", []string{"-n", "x", "-x", "y"}, nil, []string{"x"}, []string{"y"}},
		{"ranges", []string{"-n", "x1", "-x", "y1", "-n", "x2", "-x", "y2"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}},
		{"stdin", []string{}, nil, emptyKeyMinimums, emptyKeyMaximums},
		{"range-no-min", []string{"-x", "y"}, ErrorExpectedRangeInconsistent, emptyKeyMinimums, []string{"y"}},
		{"range-no-max", []string{"-n", "x"}, ErrorExpectedRangeInconsistent, []string{"x"}, emptyKeyMaximums},
	} {
		t.Run(test.name, func(t *testing.T) {
			keyMinimums = emptyKeyMinimums
			keyMaximums = emptyKeyMaximums

			buf := new(bytes.Buffer)
			Cmd.SetArgs(test.args)
			Cmd.SetOut(buf)
			Cmd.SetErr(buf)
			err := Cmd.Execute()
			assert.Equal(t, test.expectedKeyMinimums, keyMinimums)
			assert.Equal(t, test.expectedKeyMaximums, keyMaximums)
			assert.ErrorIs(t, err, test.expectedErr)

			buf.Reset()
		})
	}
}
