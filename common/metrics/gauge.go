// Copyright 2024 StreamNative, Inc.
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
	"context"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
)

type Gauge interface {
	Unregister()
}

type gauge struct {
	gauge        instrument.Int64ObservableGauge
	attrs        instrument.MeasurementOption
	callback     func() int64
	registration metric.Registration
}

func (g *gauge) Unregister() {
	if err := g.registration.Unregister(); err != nil {
		log.Fatal().Err(err).Msg("Failed to unregister gauge")
	}
}

func NewGauge(name string, description string, unit Unit, labels map[string]any, callback func() int64) Gauge {
	g, err := meter.Int64ObservableGauge(name,
		instrument.WithUnit(string(unit)),
		instrument.WithDescription(description),
	)
	fatalOnErr(err, name)

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
		log.Fatal().Err(err).Msg("Failed to register gauge")
	}
	return res
}
