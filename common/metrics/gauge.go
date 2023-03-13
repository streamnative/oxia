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

package metrics

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"sync"
)

type Gauge interface {
	Unregister()
}

type gauge struct {
	id       int64
	gauge    instrument.Int64ObservableGauge
	attrs    []attribute.KeyValue
	callback func() int64
}

func (g *gauge) Unregister() {
	registry.remove(g.id)
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

	res.id = registry.add(res)
	return res
}

type gaugesRegistry struct {
	sync.Mutex

	gauges map[int64]*gauge
	idGen  int64
}

func (r *gaugesRegistry) register() {
	_, err := meter.RegisterCallback(r.callback)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to register gauges registry")
	}
}

func (r *gaugesRegistry) callback(ctx context.Context, obs metric.Observer) error {
	r.Lock()
	defer r.Unlock()

	for _, g := range r.gauges {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		obs.ObserveInt64(g.gauge, g.callback(), g.attrs...)
	}
	return nil
}

func (r *gaugesRegistry) add(g *gauge) int64 {
	r.Lock()
	defer r.Unlock()

	id := r.idGen
	r.idGen++
	r.gauges[id] = g
	return id
}

func (r *gaugesRegistry) remove(id int64) {
	r.Lock()
	defer r.Unlock()

	delete(r.gauges, id)
}

var registry = &gaugesRegistry{
	gauges: map[int64]*gauge{},
}
