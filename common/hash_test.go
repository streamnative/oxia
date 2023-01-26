package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestXxh332(t *testing.T) {
	for _, test := range []struct {
		key      string
		expected uint32
	}{
		{"foo", 125730186},
		{"bar", 2687685474},
		{"baz", 862947621},
	} {
		t.Run(test.key, func(t *testing.T) {
			hash := Xxh332(test.key)
			assert.Equal(t, test.expected, hash)
		})
	}
}
