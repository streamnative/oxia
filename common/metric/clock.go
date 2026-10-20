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
	"sync/atomic"
	"time"
)

// The timers read the CPU's counter where it ticks at a constant rate and is
// readable from user space: the invariant TSC on amd64, CNTVCT_EL0 on arm64.
// A read takes a few ns, while time.Since is a libc or vDSO call of 17-25 ns.
// Elsewhere they read time.Since(timerEpoch), in ns: that reads only the
// monotonic clock, while time.Now also reads the wall clock.
//
// The counter's rate is measured against the monotonic clock from the process
// start, without blocking: a timer done before calibrationSpan has passed
// measures it with a monotonic read of its own.

const (
	calibrationSpan = 100 * time.Millisecond
	microsPerNano   = 1 / float64(time.Microsecond)
)

var (
	timerEpoch = time.Now()

	epochTicks, epochMono = tickPair()

	// microsPerTick holds the float64 bits of the µs per tick, or 0 until the
	// counter is calibrated.
	microsPerTick atomic.Uint64
)

func init() {
	if !counterOK {
		// coverage-ignore: runs only without an invariant TSC, which the CI CPUs have
		microsPerTick.Store(math.Float64bits(microsPerNano))
	}
}

func timerTicks() int64 {
	if !counterOK {
		// coverage-ignore: runs only without an invariant TSC, which the CI CPUs have
		return int64(time.Since(timerEpoch))
	}
	return readCounter()
}

// elapsedMicros returns the µs elapsed since start, a timerTicks reading.
func elapsedMicros(start int64) int64 {
	// A counter read on another core may be a few ticks behind.
	ticks := max(timerTicks()-start, 0)
	rate := math.Float64frombits(microsPerTick.Load())
	if rate == 0 {
		rate = calibrate()
	}
	return int64(float64(ticks) * rate)
}

// calibrate returns the µs per tick measured since the process start, and keeps
// it once the span is long enough for the reads' jitter not to matter.
func calibrate() float64 {
	ticks, mono := tickPair()
	ticks -= epochTicks
	mono -= epochMono
	if ticks <= 0 {
		return 0
	}
	rate := float64(mono) * microsPerNano / float64(ticks)
	if mono >= calibrationSpan {
		microsPerTick.Store(math.Float64bits(rate))
	}
	return rate
}

// tickPair reads the ticks between two monotonic reads, and keeps the closest
// of a few tries: a thread preempted between the reads would skew the rate
// for good.
func tickPair() (ticks int64, mono time.Duration) {
	window := time.Duration(math.MaxInt64)
	for range 5 {
		before := time.Since(timerEpoch)
		t := timerTicks()
		after := time.Since(timerEpoch)
		if after-before < window {
			window, ticks, mono = after-before, t, before+(after-before)/2
		}
	}
	return ticks, mono
}
