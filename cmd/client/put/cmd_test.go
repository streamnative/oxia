package put

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_exec(t *testing.T) {
	emptyKeys := []string{}
	emptyPayloads := []string{}
	emptyVersions := []int64{}
	for _, test := range []struct {
		name                   string
		args                   []string
		expectedErr            error
		expectedKeys           []string
		expectedPayloads       []string
		expectedVersions       []int64
		expectedBinaryPayloads bool
	}{
		{"entry", []string{"-k", "x", "-p", "y"}, nil, []string{"x"}, []string{"y"}, emptyVersions, false},
		{"entry-binary", []string{"-k", "x", "-p", "aGVsbG8=", "-b"}, nil, []string{"x"}, []string{"aGVsbG8="}, emptyVersions, true},
		{"entry-expected-version", []string{"-k", "x", "-p", "y", "-e", "1"}, nil, []string{"x"}, []string{"y"}, []int64{1}, false},
		{"entries", []string{"-k", "x1", "-p", "y1", "-k", "x2", "-p", "y2"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}, emptyVersions, false},
		{"entries-expected-version", []string{"-k", "x1", "-p", "y1", "-e", "1", "-k", "x2", "-p", "y2", "-e", "4"}, nil, []string{"x1", "x2"}, []string{"y1", "y2"}, []int64{1, 4}, false},
		{"stdin", []string{}, nil, emptyKeys, emptyPayloads, emptyVersions, false},
		{"stdin-binary", []string{"-b"}, nil, emptyKeys, emptyPayloads, emptyVersions, true},
		{"missing-key", []string{"-p", "y"}, ErrorExpectedKeyPayloadInconsistent, emptyKeys, []string{"y"}, emptyVersions, false},
		{"missing-payload", []string{"-k", "x"}, ErrorExpectedKeyPayloadInconsistent, []string{"x"}, emptyPayloads, emptyVersions, false},
		{"missing-version", []string{"-k", "x1", "-p", "y1", "-k", "x2", "-p", "y2", "-e", "4"}, ErrorExpectedVersionInconsistent, []string{"x1", "x2"}, []string{"y1", "y2"}, []int64{4}, false},
		{"bad-binary", []string{"-k", "x", "-p", "y", "-b"}, ErrorBase64PayloadInvalid, []string{"x"}, []string{"y"}, emptyVersions, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys = emptyKeys
			payloads = emptyPayloads
			expectedVersions = emptyVersions
			binaryPayloads = false

			buf := new(bytes.Buffer)
			Cmd.SetArgs(test.args)
			Cmd.SetOut(buf)
			Cmd.SetErr(buf)
			err := Cmd.Execute()
			assert.Equal(t, test.expectedKeys, keys)
			assert.Equal(t, test.expectedVersions, expectedVersions)
			assert.ErrorIs(t, err, test.expectedErr)

			buf.Reset()
		})
	}
}
