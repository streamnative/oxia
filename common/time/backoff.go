// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package time

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
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
