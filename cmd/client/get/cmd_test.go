package get

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_exec(t *testing.T) {
	emptyKeys := []string{}
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
		{"stdin", []string{}, nil, emptyKeys, false},
		{"stdin-binary", []string{"-b"}, nil, emptyKeys, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys = emptyKeys

			buf := new(bytes.Buffer)
			Cmd.SetArgs(test.args)
			Cmd.SetOut(buf)
			Cmd.SetErr(buf)
			err := Cmd.Execute()
			assert.Equal(t, test.expectedKeys, keys)
			assert.ErrorIs(t, err, test.expectedErr)

			buf.Reset()
		})
	}
}
