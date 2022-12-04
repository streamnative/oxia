package common

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

const (
	backoffIncreaseFactor     int   = 2
	backoffRandomRangePercent int64 = 10
)

type Backoff interface {
	WaitNext(ctx context.Context) error

	Reset()
}

func NewBackoff(initialWait, maxWait time.Duration) Backoff {
	return &backoff{
		initialWait:   initialWait,
		maxWait:       maxWait,
		nextWait:      initialWait,
		percentRandom: backoffRandomRangePercent,
	}
}

type backoff struct {
	sync.Mutex

	initialWait   time.Duration
	maxWait       time.Duration
	nextWait      time.Duration
	percentRandom int64
}

func (b *backoff) WaitNext(ctx context.Context) error {
	b.Lock()
	defer b.Unlock()

	timer := time.NewTimer(b.nextWait)

	select {
	case <-timer.C:
		break
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	}

	b.nextWait = b.computeNextWait(b.nextWait)
	return nil
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= b {
		return a
	}
	return b
}

func (b *backoff) computeNextWait(currentWait time.Duration) (nextWait time.Duration) {
	nextWait = minDuration(
		currentWait*time.Duration(backoffIncreaseFactor),
		b.maxWait,
	)

	// Introduce a ±x% factor
	percentRange := nextWait.Nanoseconds() / backoffRandomRangePercent
	nextWait += time.Duration(rand.Int63n(2*percentRange) - percentRange)
	return nextWait
}

func (b *backoff) Reset() {
	b.Lock()
	defer b.Unlock()

	b.nextWait = b.initialWait
}
