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

import "sync"

// registry holds the values of the failpoints enabled for the whole process,
// by name, for the end-to-end tests that can't reach the *Failpoints of the
// component under test.
//
// Owner: the test that enables a failpoint, and disables it when it ends.
// Borrowers: the server code, which only reads a value, with Eval.
// The sync.Map guards it.
var registry sync.Map

// Enable enables the failpoint name for the whole process, with value.
func Enable(name string, value any) {
	registry.Store(name, value)
}

// Disable disables the failpoint name.
func Disable(name string) {
	registry.Delete(name)
}

// Eval returns the value of the failpoint name, and whether it is enabled with
// a value of type T.
func Eval[T any](name string) (T, bool) {
	value, ok := registry.Load(name)
	if !ok {
		var zero T
		return zero, false
	}
	typed, ok := value.(T)
	return typed, ok
}
