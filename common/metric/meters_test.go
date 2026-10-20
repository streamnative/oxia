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

package metric

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestPanicOnErrPanicsWithMetricName(t *testing.T) {
	assert.NotPanics(t, func() { panicOnErr(nil, "metric") })
	assert.PanicsWithError(t, "failed to create metric metric: failed", func() {
		panicOnErr(errors.New("failed"), "metric")
	})
}

func TestGetAttrSetConvertsLabelTypes(t *testing.T) {
	set := getAttrSet(map[string]any{
		"uint32":  uint32(7),
		"int64":   int64(-3),
		"int":     5,
		"float64": 1.5,
		"bool":    true,
		"string":  "value",
	})

	assert.Equal(t, attribute.NewSet(
		attribute.Int64("uint32", 7),
		attribute.Int64("int64", -3),
		attribute.Int("int", 5),
		attribute.Float64("float64", 1.5),
		attribute.Bool("bool", true),
		attribute.String("string", "value"),
	), set)
}

func TestGetAttrSetPanicsOnInvalidLabelType(t *testing.T) {
	assert.PanicsWithValue(t, "invalid type int32 of metric label shard", func() {
		getAttrSet(map[string]any{"shard": int32(1)})
	})
}
