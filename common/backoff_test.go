package common

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	b := NewBackoff(1*time.Second, 3*time.Second)

	t1 := time.Now()

	err := b.WaitNext(context.Background())
	assert.NoError(t, err)

	assert.WithinDuration(t, t1.Add(1*time.Second), time.Now(), 100*time.Millisecond)

	t2 := time.Now()

	err = b.WaitNext(context.Background())
	assert.NoError(t, err)

	assert.WithinDuration(t, t2.Add(2*time.Second), time.Now(), 200*time.Millisecond)

	t3 := time.Now()

	err = b.WaitNext(context.Background())
	assert.NoError(t, err)

	assert.WithinDuration(t, t3.Add(3*time.Second), time.Now(), 300*time.Millisecond)

	t4 := time.Now()

	// Cancel the wait
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = b.WaitNext(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}()

	cancel()
	wg.Wait()
	assert.WithinDuration(t, t4, time.Now(), 100*time.Millisecond)

	t5 := time.Now()

	// Context with timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		err = b.WaitNext(ctx2)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		wg2.Done()
	}()

	wg2.Wait()
	assert.WithinDuration(t, t5, time.Now(), 200*time.Millisecond)

	// Reset to initial interval
	b.Reset()
	t6 := time.Now()
	err = b.WaitNext(context.Background())
	assert.NoError(t, err)

	assert.WithinDuration(t, t6.Add(1*time.Second), time.Now(), 100*time.Millisecond)
}
