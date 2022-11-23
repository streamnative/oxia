package common

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteOutput(t *testing.T) {
	for _, test := range []struct {
		name     string
		result   any
		expected string
	}{
		{"common.OutputError", OutputError{Err: "hello"}, "{\"error\":\"hello\"}\n"},
		{"common.OutputErrorEmpty", OutputError{}, "{}\n"},
		{"common.OutputStat", OutputStat{
			Version:           1,
			CreatedTimestamp:  2,
			ModifiedTimestamp: 3,
		}, "{\"version\":1,\"created_timestamp\":2,\"modified_timestamp\":3}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := bytes.NewBufferString("")
			writeOutput(b, test.result)
			assert.Equal(t, test.expected, b.String())
		})
	}
}
