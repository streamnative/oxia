package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitSet(t *testing.T) {
	bs := BitSet{}

	assert.Zero(t, bs.Count())

	bs.Set(0)
	assert.Equal(t, 1, bs.Count())

	bs.Set(2)
	assert.Equal(t, 2, bs.Count())

	bs.Set(1)
	assert.Equal(t, 3, bs.Count())

	bs.Set(2)
	assert.Equal(t, 3, bs.Count())
}

func TestBitSetPanic(t *testing.T) {
	bs := BitSet{}

	assert.Panics(t, func() {
		bs.Set(-2)
	})

	assert.Panics(t, func() {
		bs.Set(16)
	})

	assert.Panics(t, func() {
		bs.Set(20)
	})
}
