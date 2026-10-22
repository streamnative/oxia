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
	"log/slog"
)

// maxLevelHandler logs the records above its level at its level.
type maxLevelHandler struct {
	slog.Handler
	level slog.Level
}

func (h *maxLevelHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level > h.level {
		record.Level = h.level
	}
	return h.Handler.Handle(ctx, record)
}

func (h *maxLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &maxLevelHandler{Handler: h.Handler.WithAttrs(attrs), level: h.level}
}

func (h *maxLevelHandler) WithGroup(name string) slog.Handler {
	return &maxLevelHandler{Handler: h.Handler.WithGroup(name), level: h.level}
}
