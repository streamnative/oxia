package common

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"time"
)

func NewBackOff(ctx context.Context) backoff.BackOff {
	return NewBackOffWithInitialInterval(ctx, 100*time.Millisecond)
}

func NewBackOffWithInitialInterval(ctx context.Context, initialInterval time.Duration) backoff.BackOff {
	return backoff.WithContext(&backoff.ExponentialBackOff{
		InitialInterval:     initialInterval,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         backoff.DefaultMaxInterval,
		MaxElapsedTime:      0, // Never stop trying
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}, ctx)
}
