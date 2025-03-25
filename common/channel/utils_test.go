package channel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPollWithData(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 99
	val, ok := Poll(ch)
	assert.True(t, ok)
	assert.EqualValues(t, 99, val)
	val, ok = Poll(ch)
	assert.False(t, ok)
}

func TestPollWithoutData(t *testing.T) {
	ch := make(chan int)
	_, ok := Poll(ch)
	assert.False(t, ok)
}
