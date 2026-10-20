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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestGaugeUnregisterStopsObserving(t *testing.T) {
	reader := withTestMeter(t)
	g := NewGauge("gauge", "", Dimensionless, map[string]any{"shard": int64(1)}, func() int64 { return 42 })

	points := gaugeDataPoints(t, collect(t, reader))
	require.Len(t, points, 1)
	assert.EqualValues(t, 42, points[0].Value)
	assert.Equal(t, attribute.NewSet(attribute.Int64("shard", 1)), points[0].Attributes)

	g.Unregister()
	assert.Empty(t, collect(t, reader).ScopeMetrics)
}

func TestGaugeUnregisterLogsFailure(t *testing.T) {
	logs := captureLogs(t)
	g := &gauge{registration: &fakeRegistration{
		unregister: func() error { return errors.New("unregister failed") },
	}}

	g.Unregister()

	assert.Contains(t, logs.String(), "Failed to unregister gauge")
	assert.Contains(t, logs.String(), "unregister failed")
}

func TestNewGaugeCallbackSkipsCanceledCollection(t *testing.T) {
	reader := withTestMeter(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The SDK runs the callbacks in registration order: this one cancels the
	// collection before the gauge callback runs.
	canceler, err := GetMeter().Int64ObservableGauge("canceler")
	require.NoError(t, err)
	registration, err := GetMeter().RegisterCallback(func(context.Context, metric.Observer) error {
		cancel()
		return nil
	}, canceler)
	require.NoError(t, err)
	defer func() { require.NoError(t, registration.Unregister()) }()

	calls := atomic.Int32{}
	g := NewGauge("gauge", "", Dimensionless, map[string]any{}, func() int64 {
		calls.Add(1)
		return 1
	})
	defer g.Unregister()

	var rm metricdata.ResourceMetrics
	assert.ErrorIs(t, reader.Collect(ctx, &rm), context.Canceled)
	assert.Zero(t, calls.Load())
}

func TestNewGaugePanicsOnInvalidName(t *testing.T) {
	withTestMeter(t)

	assert.PanicsWithError(t, "failed to create metric 1gauge: invalid instrument name: 1gauge: must start with a letter",
		func() { NewGauge("1gauge", "", Dimensionless, map[string]any{}, func() int64 { return 0 }) })
}

func TestNewGaugePanicsOnRegisterFailure(t *testing.T) {
	previous := GetMeter()
	t.Cleanup(func() { SetMeter(previous) })
	SetMeter(&fakeMeter{registerCallback: func(metric.Callback, ...metric.Observable) (metric.Registration, error) {
		return nil, errors.New("register failed")
	}})

	assert.PanicsWithError(t, "failed to register gauge gauge: register failed",
		func() { NewGauge("gauge", "", Dimensionless, map[string]any{}, func() int64 { return 0 }) })
}

func TestNewSyncGaugePanicsOnInvalidName(t *testing.T) {
	withTestMeter(t)

	assert.PanicsWithError(t, "failed to create metric 1gauge: invalid instrument name: 1gauge: must start with a letter",
		func() { NewSyncGauge("1gauge", "", Dimensionless, map[string]any{}) })
}

func TestSyncGaugeRecord(t *testing.T) {
	reader := withTestMeter(t)
	g := NewSyncGauge("sync-gauge", "", Dimensionless, map[string]any{"shard": int64(1)})

	g.Record(5)
	g.Record(7, attribute.String("kind", "extra"))

	points := gaugeDataPoints(t, collect(t, reader))
	values := map[attribute.Set]int64{}
	for _, p := range points {
		values[p.Attributes] = p.Value
	}
	assert.Equal(t, map[attribute.Set]int64{
		attribute.NewSet(attribute.Int64("shard", 1)):                                    5,
		attribute.NewSet(attribute.Int64("shard", 1), attribute.String("kind", "extra")): 7,
	}, values)
}

// withTestMeter routes the metrics created by the test to a fresh SDK meter,
// and restores the previous meter when the test ends.
func withTestMeter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	previous := GetMeter()
	t.Cleanup(func() { SetMeter(previous) })

	reader := sdkmetric.NewManualReader()
	SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	return reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func gaugeDataPoints(t *testing.T, rm metricdata.ResourceMetrics) []metricdata.DataPoint[int64] {
	t.Helper()
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
	data, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	return data.DataPoints
}

// captureLogs routes the default logger to the returned buffer until the test ends.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	logs := &bytes.Buffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	return logs
}

type fakeMeter struct {
	noop.Meter
	registerCallback func(metric.Callback, ...metric.Observable) (metric.Registration, error)
}

func (m *fakeMeter) RegisterCallback(f metric.Callback, insts ...metric.Observable) (metric.Registration, error) {
	return m.registerCallback(f, insts...)
}

type fakeRegistration struct {
	noop.Registration
	unregister func() error
}

func (r *fakeRegistration) Unregister() error {
	return r.unregister()
}
