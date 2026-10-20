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
	"os"
	"os/signal"
	"syscall"
	"time"
)

// RunProcess starts the process and the profiler, and closes them once ctx is
// done or the process receives SIGINT or SIGTERM. A signal received while the
// process starts closes it as soon as it has started. It logs when the process
// named name is ready, and when it stopped.
func RunProcess(ctx context.Context, name string, startProcess func() (io.Closer, error)) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	start := time.Now()
	profiler := RunProfiling(ctx)
	process, err := startProcess()
	if err != nil {
		return errors.Join(fmt.Errorf("failed to start the process: %w", err), profiler.Close())
	}
	slog.Info(fmt.Sprintf("Oxia %s is ready", name), slog.String("startup", time.Since(start).Round(time.Millisecond).String()))

	<-ctx.Done()
	slog.Info(fmt.Sprintf("Oxia %s is shutting down", name), slog.Any("cause", context.Cause(ctx)))
	if err := errors.Join(process.Close(), profiler.Close()); err != nil {
		return fmt.Errorf("failed to shut down the process: %w", err)
	}
	slog.Info(fmt.Sprintf("Oxia %s stopped", name), slog.String("uptime", time.Since(start).Round(time.Second).String()))
	return nil
}
