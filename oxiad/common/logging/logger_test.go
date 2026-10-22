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

package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/common/option"
)

func TestReconfigureLogger_DynamicLevelChange(t *testing.T) {
	// Start with INFO level
	LogLevel = slog.LevelInfo
	LogJSON = true
	ConfigureLogger()

	// Create a derived logger before the level change, simulating
	// component loggers created at startup via slog.With().
	derivedLogger := slog.Default().With("component", "test")

	// The derived logger should not enable DEBUG at INFO level
	assert.False(t, derivedLogger.Enabled(context.Background(), slog.LevelDebug))
	assert.True(t, derivedLogger.Enabled(context.Background(), slog.LevelInfo))

	// Reconfigure to DEBUG level
	changed := ReconfigureLogger(&option.LogOptions{Level: "debug"})
	assert.True(t, changed)

	// The derived logger (created before reconfigure) should now enable DEBUG
	assert.True(t, derivedLogger.Enabled(context.Background(), slog.LevelDebug))
}

func TestReconfigureLogger_NoChangeReturnsFalse(t *testing.T) {
	LogLevel = slog.LevelInfo
	LogJSON = true
	ConfigureLogger()

	changed := ReconfigureLogger(&option.LogOptions{Level: "info"})
	assert.False(t, changed)
}

func TestConfigureLoggerWithoutJSONAppliesLevel(t *testing.T) {
	defer func(level slog.Level, json bool) {
		LogLevel = level
		LogJSON = json
		ConfigureLogger()
	}(LogLevel, LogJSON)

	LogLevel = slog.LevelWarn
	LogJSON = false
	ConfigureLogger()

	assert.False(t, slog.Default().Enabled(context.Background(), slog.LevelInfo))
	assert.True(t, slog.Default().Enabled(context.Background(), slog.LevelWarn))
}

func TestConfigureLoggerWritesDurationsInMilliseconds(t *testing.T) {
	stdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = stdout
		ConfigureLogger()
	})

	LogLevel = slog.LevelInfo
	LogJSON = true
	ConfigureLogger()
	slog.Info("retrying", slog.Duration("retry-after", 2414989107*time.Nanosecond), slog.Int("attempt", 3))
	require.NoError(t, w.Close())

	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"retry-after":2414,`)
	assert.Contains(t, string(out), `"attempt":3`)
}
