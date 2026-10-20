// Copyright 2023-2026 The Oxia Authors
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

package metric

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The elapsed µs agree with the monotonic clock, whether the counter is
// calibrated yet or not.
func TestElapsedMicrosMatchesMonotonicClock(t *testing.T) {
	check := func() {
		outer := time.Now()
		start := timerTicks()
		inner := time.Now()
		time.Sleep(5 * time.Millisecond)
		innerMicros := time.Since(inner).Microseconds()
		micros := elapsedMicros(start)
		outerMicros := time.Since(outer).Microseconds()
		require.GreaterOrEqual(t, micros, innerMicros*99/100)
		require.LessOrEqual(t, micros, outerMicros*101/100+1)
	}
	check()

	time.Sleep(time.Until(timerEpoch.Add(calibrationSpan)))
	check()
	require.NotZero(t, microsPerTick.Load())
	check()
}

// A counter read on another core may be behind the one of the process start:
// it gives no rate yet.
func TestCalibrateReturnsZeroWithoutTicks(t *testing.T) {
	startTicks := epochTicks
	epochTicks = math.MaxInt64
	defer func() { epochTicks = startTicks }()

	require.Zero(t, calibrate())
}
