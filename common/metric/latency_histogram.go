// Copyright 2023 StreamNative, Inc.
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
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
)

var latencyBucketsMillis = []float64{
	0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000,
}

type Timer struct {
	histo *latencyHistogram
	start time.Time
}

func (tm Timer) Done() {
	tm.histo.histo.Record(context.Background(), float64(time.Since(tm.start).Microseconds())/1000.0, tm.histo.attrs)
}

type LatencyHistogram interface {
	Timer() Timer
}

type latencyHistogram struct {
	histo metric.Float64Histogram
	attrs metric.MeasurementOption
}

func (t *latencyHistogram) Timer() Timer {
	return Timer{t, time.Now()}
}

func NewLatencyHistogram(name string, description string, labels map[string]any) LatencyHistogram {
	h, err := meter.Float64Histogram(
		name,
		metric.WithUnit(string(Milliseconds)),
		metric.WithDescription(description),
	)
	fatalOnErr(err, name)

	return &latencyHistogram{histo: h, attrs: getAttrs(labels)}
}
