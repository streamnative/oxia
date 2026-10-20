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

// Package failpoint injects failures into the server components, for the
// tests. The builds with the disable_trap tag compile it to a no-op.
package failpoint

// Failpoints holds the hooks of the failpoints, by name. A nil *Failpoints has
// none.
type Failpoints struct {
	hooks map[string]func() error
}

func New(hooks map[string]func() error) *Failpoints {
	return &Failpoints{
		hooks: hooks,
	}
}

// Trigger runs the hook of the failpoint name and returns its error, or nil
// when the failpoint has no hook.
func (f *Failpoints) Trigger(name string) error {
	if f == nil {
		return nil
	}
	if h := f.hooks[name]; h != nil {
		return h()
	}
	return nil
}

// Fail returns err, the result of the call at the failpoint name, unless the
// hook of the failpoint returns an error, which replaces it. The caller then
// still has to release what the call returned.
func (f *Failpoints) Fail(name string, err error) error {
	if hookErr := f.Trigger(name); hookErr != nil {
		return hookErr
	}
	return err
}
