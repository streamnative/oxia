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

package kvstore

import (
	"fmt"
	"log/slog"
)

// pebbleLogger is the wrapper of slog to implement pebbel's logger interface.
type pebbleLogger struct {
	zl *slog.Logger
}

func (pl *pebbleLogger) Infof(format string, args ...any) {
	pl.zl.Info(
		fmt.Sprintf(format, args...),
	)
}

func (pl *pebbleLogger) Errorf(format string, args ...any) {
	pl.zl.Error(
		fmt.Sprintf(format, args...),
	)
}

// Fatalf panics after logging: Pebble calls it on corruption or a broken
// invariant, and continues as if it did not return.
func (pl *pebbleLogger) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	pl.zl.Error(msg)
	panic(msg)
}
