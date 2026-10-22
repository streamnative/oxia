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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
)

func TestMaxLevelHandler(t *testing.T) {
	var buf bytes.Buffer
	log := logr.FromSlogHandler(&maxLevelHandler{
		Handler: slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		level:   slog.LevelWarn,
	}).WithName("leader-election").WithValues("lock", "oxia-status")

	log.Error(errors.New("connection reset by peer"), "Failed to update lock optimistically")
	assert.Contains(t, buf.String(), "level=WARN")
	assert.Contains(t, buf.String(), "lock=oxia-status")

	buf.Reset()
	log.Info("successfully acquired lease")
	assert.Contains(t, buf.String(), "level=INFO")

	buf.Reset()
	handler := &maxLevelHandler{Handler: slog.NewTextHandler(&buf, nil), level: slog.LevelWarn}
	slog.New(handler.WithGroup("k8s")).ErrorContext(context.Background(), "failed", slog.String("lock", "oxia-status"))
	assert.Contains(t, buf.String(), "level=WARN")
	assert.Contains(t, buf.String(), "k8s.lock=oxia-status")
}
