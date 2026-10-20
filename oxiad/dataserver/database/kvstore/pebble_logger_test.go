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
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPebbleLoggerInfof(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := &pebbleLogger{slog.New(slog.NewTextHandler(logs, nil))}

	logger.Infof("opened %s", "db")

	assert.Contains(t, logs.String(), `level=INFO msg="opened db"`)
}

func TestPebbleLoggerErrorf(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := &pebbleLogger{slog.New(slog.NewTextHandler(logs, nil))}

	logger.Errorf("compaction %d failed", 3)

	assert.Contains(t, logs.String(), `level=ERROR msg="compaction 3 failed"`)
}

func TestPebbleLoggerFatalfPanicsAfterLogging(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := &pebbleLogger{slog.New(slog.NewTextHandler(logs, nil))}

	assert.PanicsWithValue(t, "MANIFEST sync failed: disk", func() {
		logger.Fatalf("MANIFEST sync failed: %s", "disk")
	})
	assert.Contains(t, logs.String(), `level=ERROR msg="MANIFEST sync failed: disk"`)
}
