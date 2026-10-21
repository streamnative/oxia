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

//go:build !disable_trap

package failpoint

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnableSetsTheValue(t *testing.T) {
	Enable("point", time.Second)
	t.Cleanup(func() { Disable("point") })

	value, ok := Eval[time.Duration]("point")
	assert.True(t, ok)
	assert.Equal(t, time.Second, value)
}

func TestDisableRemovesTheValue(t *testing.T) {
	Enable("point", time.Second)
	Disable("point")

	value, ok := Eval[time.Duration]("point")
	assert.False(t, ok)
	assert.Zero(t, value)
}

func TestEvalWithoutValue(t *testing.T) {
	value, ok := Eval[time.Duration]("other")
	assert.False(t, ok)
	assert.Zero(t, value)
}

func TestEvalWithValueOfAnotherType(t *testing.T) {
	Enable("point", "1s")
	t.Cleanup(func() { Disable("point") })

	value, ok := Eval[time.Duration]("point")
	assert.False(t, ok)
	assert.Zero(t, value)
}

func TestEvalWhileEnabling(t *testing.T) {
	t.Cleanup(func() { Disable("point") })
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 1; i <= 100; i++ {
			Enable("point", i)
			Disable("point")
		}
	})
	for range 100 {
		if value, ok := Eval[int]("point"); ok {
			assert.Positive(t, value)
		}
	}
	wg.Wait()
}
