package common

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoize(t *testing.T) {
	count := atomic.Int64{}

	f := func() int64 {
		return count.Add(1)
	}

	m := Memoize(f, 1*time.Second)

	for i := 0; i < 10; i++ {
		assert.EqualValues(t, 1, m())
	}

	// Let the cached value expire
	time.Sleep(1100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		assert.EqualValues(t, 2, m())
	}
}
