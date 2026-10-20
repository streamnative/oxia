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
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Gauge interface {
	Unregister()
}

type gauge struct {
	gauge        metric.Int64ObservableGauge
	attrs        metric.MeasurementOption
	callback     func() int64
	registration metric.Registration
}

// Unregister stops observing the gauge. A failure is logged: it leaves a stale
// series behind, which does not affect the component that is closing.
func (g *gauge) Unregister() {
	if err := g.registration.Unregister(); err != nil {
		slog.Error(
			"Failed to unregister gauge",
			slog.Any("error", err),
		)
	}
}

type SyncGauge interface {
	Record(value int64, attrs ...attribute.KeyValue)
}

type syncGauge struct {
	gauge metric.Int64Gauge
	attrs metric.MeasurementOption
}

func (g *syncGauge) Record(value int64, attrs ...attribute.KeyValue) {
	if len(attrs) > 0 {
		g.gauge.Record(context.Background(), value, g.attrs, metric.WithAttributes(attrs...))
	} else {
		g.gauge.Record(context.Background(), value, g.attrs)
	}
}

func NewSyncGauge(name string, description string, unit Unit, labels map[string]any) SyncGauge {
	g, err := GetMeter().Int64Gauge(name,
		metric.WithUnit(string(unit)),
		metric.WithDescription(description),
	)
	panicOnErr(err, name)
	return &syncGauge{
		gauge: g,
		attrs: getAttrs(labels),
	}
}

func NewGauge(name string, description string, unit Unit, labels map[string]any, callback func() int64) Gauge {
	g, err := GetMeter().Int64ObservableGauge(name,
		metric.WithUnit(string(unit)),
		metric.WithDescription(description),
	)
	panicOnErr(err, name)

	res := &gauge{
		gauge:    g,
		attrs:    getAttrs(labels),
		callback: callback,
	}

	res.registration, err = meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		obs.ObserveInt64(res.gauge, res.callback(), res.attrs)
		return nil
	}, g)

	if err != nil {
		panic(fmt.Errorf("failed to register gauge %s: %w", name, err))
	}
	return res
}
