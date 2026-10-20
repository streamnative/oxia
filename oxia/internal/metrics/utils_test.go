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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/oxia-db/oxia/common/proto"
)

const invalidNameError = "failed to create oxia client metric 1metric: invalid instrument name: 1metric: " +
	"must start with a letter"

func TestNewHistogramPanicsOnInvalidName(t *testing.T) {
	meter := metric.NewMeterProvider().Meter("test")

	assert.PanicsWithError(t, invalidNameError, func() { newHistogram(meter, "1metric", "") })
}

func TestNewMillisCounterPanicsOnInvalidName(t *testing.T) {
	meter := metric.NewMeterProvider().Meter("test")

	assert.PanicsWithError(t, invalidNameError, func() { newMillisCounter(meter, "1metric") })
}

func TestNewCounterPanicsOnInvalidName(t *testing.T) {
	meter := metric.NewMeterProvider().Meter("test")

	assert.PanicsWithError(t, invalidNameError, func() { newCounter(meter, "1metric", "") })
}

func TestReadMetrics(t *testing.T) {
	valueSize, requestCount := readMetrics(&proto.ReadResponse{Gets: []*proto.GetResponse{
		{Value: []byte{0, 1, 2}},
		{Value: []byte{3}},
	}})
	assert.EqualValues(t, 4, valueSize)
	assert.EqualValues(t, 2, requestCount)
}

// A failed read has no response.
func TestReadMetricsWithoutResponse(t *testing.T) {
	valueSize, requestCount := readMetrics(nil)
	assert.Zero(t, valueSize)
	assert.Zero(t, requestCount)
}
