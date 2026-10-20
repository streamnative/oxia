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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFailpointsTriggerRunsHook(t *testing.T) {
	hookErr := errors.New("hook")
	calls := 0
	f := New(map[string]func() error{
		"point": func() error {
			calls++
			return hookErr
		},
	})

	assert.ErrorIs(t, f.Trigger("point"), hookErr)
	assert.Equal(t, 1, calls)
}

func TestFailpointsTriggerWithoutHook(t *testing.T) {
	f := New(map[string]func() error{
		"point": func() error { return errors.New("hook") },
	})

	assert.NoError(t, f.Trigger("other"))
}

func TestFailpointsTriggerNil(t *testing.T) {
	var f *Failpoints

	assert.NoError(t, f.Trigger("point"))
}

func TestFailpointsFailReplacesResult(t *testing.T) {
	hookErr := errors.New("hook")
	f := New(map[string]func() error{
		"point": func() error { return hookErr },
	})

	assert.ErrorIs(t, f.Fail("point", nil), hookErr)
	assert.ErrorIs(t, f.Fail("point", errors.New("call")), hookErr)
}

func TestFailpointsFailKeepsResult(t *testing.T) {
	callErr := errors.New("call")
	f := New(map[string]func() error{
		"point": func() error { return nil },
	})

	assert.NoError(t, f.Fail("point", nil))
	assert.ErrorIs(t, f.Fail("point", callErr), callErr)
	assert.ErrorIs(t, f.Fail("other", callErr), callErr)
}

func TestFailpointsFailNil(t *testing.T) {
	var f *Failpoints
	callErr := errors.New("call")

	assert.NoError(t, f.Fail("point", nil))
	assert.ErrorIs(t, f.Fail("point", callErr), callErr)
}
