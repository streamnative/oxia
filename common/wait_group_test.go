package common

import (
	"context"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

func assertNotReady(t *testing.T, wg WaitGroup) {
	done := make(chan bool, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		assert.ErrorIs(t, wg.Wait(ctx), context.Canceled)
		done <- true
	}()

	select {
	case <-done:
		assert.Fail(t, "should have timed out")

	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

func TestWaitGroup(t *testing.T) {
	wg := NewWaitGroup(3)
	assertNotReady(t, wg)

	wg = NewWaitGroup(1)
	assertNotReady(t, wg)
	wg.Done()
	assert.NoError(t, wg.Wait(context.Background()))

	wg = NewWaitGroup(3)
	wg.Done()
	wg.Done()
	assertNotReady(t, wg)

	wg = NewWaitGroup(3)
	assertNotReady(t, wg)
	wg.Done()
	wg.Done()
	wg.Done()
	assert.NoError(t, wg.Wait(context.Background()))

	wg = NewWaitGroup(3)
	wg.Fail(io.EOF)
	assert.ErrorIs(t, wg.Wait(context.Background()), io.EOF)

	wg = NewWaitGroup(3)
	wg.Done()
	wg.Fail(io.EOF)
	assert.ErrorIs(t, wg.Wait(context.Background()), io.EOF)
}

func TestWaitGroup_Cancel(t *testing.T) {
	wg := NewWaitGroup(3)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)

	go func() {
		assert.ErrorIs(t, wg.Wait(ctx), context.Canceled)
		done <- true
	}()

	cancel()
	select {
	case <-done:
	// Ok
	case <-time.After(1 * time.Second):
		assert.Fail(t, "shouldn't have timed out")
	}
}

func TestWaitGroup_Timeout(t *testing.T) {
	wg := NewWaitGroup(3)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan bool)

	go func() {
		assert.ErrorIs(t, wg.Wait(ctx), context.DeadlineExceeded)
		done <- true
	}()

	select {
	case <-done:
	// Ok
	case <-time.After(1 * time.Second):
		assert.Fail(t, "shouldn't have timed out")
	}
}
