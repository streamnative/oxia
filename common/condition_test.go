package common

import (
	"context"
	"github.com/stretchr/testify/assert"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCondCancelContext(t *testing.T) {
	var m sync.Mutex
	c := NewConditionContext(&m)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		m.Lock()
		defer m.Unlock()

		err := c.Wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}()

	cancel()
	wg.Wait()
}

func TestCondContextWithTimeout(t *testing.T) {
	var m sync.Mutex
	c := NewConditionContext(&m)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		m.Lock()
		defer m.Unlock()

		err := c.Wait(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		wg.Done()
	}()

	wg.Wait()
	cancel()
}

// The rest of tests are taken from `sync.Cond` tests from stdlib

func TestCondSignal(t *testing.T) {
	var m sync.Mutex
	c := NewConditionContext(&m)
	n := 2
	running := make(chan bool, n)
	awake := make(chan bool, n)
	for i := 0; i < n; i++ {
		go func() {
			m.Lock()
			running <- true
			c.Wait(context.Background())
			awake <- true
			m.Unlock()
		}()
	}
	for i := 0; i < n; i++ {
		<-running // Wait for everyone to run.
	}
	for n > 0 {
		select {
		case <-awake:
			t.Fatal("goroutine not asleep")
		default:
		}
		m.Lock()
		c.Signal()
		m.Unlock()
		<-awake // Will deadlock if no goroutine wakes up
		select {
		case <-awake:
			t.Fatal("too many goroutines awake")
		default:
		}
		n--
	}
	c.Signal()
}

func TestCondSignalGenerations(t *testing.T) {
	var m sync.Mutex
	c := NewConditionContext(&m)
	n := 100
	running := make(chan bool, n)
	awake := make(chan int, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			m.Lock()
			running <- true
			c.Wait(context.Background())
			awake <- i
			m.Unlock()
		}(i)
		if i > 0 {
			a := <-awake
			if a != i-1 {
				t.Fatalf("wrong goroutine woke up: want %d, got %d", i-1, a)
			}
		}
		<-running
		m.Lock()
		c.Signal()
		m.Unlock()
	}
}

func TestCondBroadcast(t *testing.T) {
	var m sync.Mutex
	c := NewConditionContext(&m)
	n := 200
	running := make(chan int, n)
	awake := make(chan int, n)
	exit := false
	for i := 0; i < n; i++ {
		go func(g int) {
			m.Lock()
			for !exit {
				running <- g
				c.Wait(context.Background())
				awake <- g
			}
			m.Unlock()
		}(i)
	}
	for i := 0; i < n; i++ {
		for i := 0; i < n; i++ {
			<-running // Will deadlock unless n are running.
		}
		if i == n-1 {
			m.Lock()
			exit = true
			m.Unlock()
		}
		select {
		case <-awake:
			t.Fatal("goroutine not asleep")
		default:
		}
		m.Lock()
		c.Broadcast()
		m.Unlock()
		seen := make([]bool, n)
		for i := 0; i < n; i++ {
			g := <-awake
			if seen[g] {
				t.Fatal("goroutine woke up twice")
			}
			seen[g] = true
		}
	}
	select {
	case <-running:
		t.Fatal("goroutine did not exit")
	default:
	}
	c.Broadcast()
}

func TestRace(t *testing.T) {
	x := 0
	m := &sync.Mutex{}
	c := NewConditionContext(m)
	done := make(chan bool)
	go func() {
		m.Lock()
		x = 1
		c.Wait(context.Background())
		if x != 2 {
			t.Error("want 2")
		}
		x = 3
		c.Signal()
		m.Unlock()
		done <- true
	}()
	go func() {
		m.Lock()
		for {
			if x == 1 {
				x = 2
				c.Signal()
				break
			}
			m.Unlock()
			runtime.Gosched()
			m.Lock()
		}
		m.Unlock()
		done <- true
	}()
	go func() {
		m.Lock()
		for {
			if x == 2 {
				c.Wait(context.Background())
				if x != 3 {
					t.Error("want 3")
				}
				break
			}
			if x == 3 {
				break
			}
			m.Unlock()
			runtime.Gosched()
			m.Lock()
		}
		m.Unlock()
		done <- true
	}()
	<-done
	<-done
	<-done
}

func TestCondSignalStealing(t *testing.T) {
	for iters := 0; iters < 1000; iters++ {
		var m sync.Mutex
		cond := NewConditionContext(&m)

		// Start a waiter.
		ch := make(chan struct{})
		go func() {
			m.Lock()
			ch <- struct{}{}
			cond.Wait(context.Background())
			m.Unlock()

			ch <- struct{}{}
		}()

		<-ch
		m.Lock()
		m.Unlock()

		// We know that the waiter is in the cond.Wait() call because we
		// synchronized with it, then acquired/released the mutex it was
		// holding when we synchronized.
		//
		// Start two goroutines that will race: one will broadcast on
		// the cond var, the other will wait on it.
		//
		// The new waiter may or may not get notified, but the first one
		// has to be notified.
		done := false
		go func() {
			cond.Broadcast()
		}()

		go func() {
			m.Lock()
			for !done {
				cond.Wait(context.Background())
			}
			m.Unlock()
		}()

		// Check that the first waiter does get signaled.
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("First waiter didn't get broadcast.")
		}

		// Release the second waiter in case it didn't get the
		// broadcast.
		m.Lock()
		done = true
		m.Unlock()
		cond.Broadcast()
	}
}

func BenchmarkCond1(b *testing.B) {
	benchmarkCond(b, 1)
}

func BenchmarkCond2(b *testing.B) {
	benchmarkCond(b, 2)
}

func BenchmarkCond4(b *testing.B) {
	benchmarkCond(b, 4)
}

func BenchmarkCond8(b *testing.B) {
	benchmarkCond(b, 8)
}

func BenchmarkCond16(b *testing.B) {
	benchmarkCond(b, 16)
}

func BenchmarkCond32(b *testing.B) {
	benchmarkCond(b, 32)
}

func benchmarkCond(b *testing.B, waiters int) {
	m := &sync.Mutex{}
	c := NewConditionContext(m)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan bool)
	id := 0

	for routine := 0; routine < waiters+1; routine++ {
		go func() {
			for i := 0; i < b.N; i++ {
				m.Lock()
				if id == -1 {
					m.Unlock()
					break
				}
				id++
				if id == waiters+1 {
					id = 0
					c.Broadcast()
				} else {
					c.Wait(ctx)
				}
				m.Unlock()
			}
			m.Lock()
			id = -1
			c.Broadcast()
			m.Unlock()
			done <- true
		}()
	}
	for routine := 0; routine < waiters+1; routine++ {
		<-done
	}
}
