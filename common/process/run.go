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

package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// RunProcess starts the process and the profiler, and closes them once ctx is
// done, e.g. when the process receives a termination signal.
func RunProcess(ctx context.Context, startProcess func() (io.Closer, error)) error {
	profiler := RunProfiling(ctx)
	process, err := startProcess()
	if err != nil {
		return errors.Join(fmt.Errorf("failed to start the process: %w", err), profiler.Close())
	}

	<-ctx.Done()
	slog.Info("Shutting down", slog.Any("cause", context.Cause(ctx)))
	if err := errors.Join(process.Close(), profiler.Close()); err != nil {
		return fmt.Errorf("failed to shut down the process: %w", err)
	}
	slog.Info("Shutdown Completed")
	return nil
}
