package metrics

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
)

// Counter is a monotonically increasing counter
type Counter interface {
	Inc()
	Add(incr int)
}

type counter struct {
	sc    syncint64.Counter
	attrs []attribute.KeyValue
}

func (c *counter) Inc() {
	c.Add(1)
}

func (c *counter) Add(incr int) {
	c.sc.Add(context.Background(), int64(incr), c.attrs...)
}

func NewCounter(name string, description string, unit unit.Unit, labels map[string]any) Counter {
	sc, err := meter.SyncInt64().Counter(name,
		instrument.WithUnit(unit),
		instrument.WithDescription(description))
	fatalOnErr(err, name)
	return &counter{
		sc:    sc,
		attrs: getAttrs(labels),
	}
}

// UpDownCounter is a counter that is incremented and decremented
// to report the current state
type UpDownCounter interface {
	Counter
	Dec()
	Sub(diff int)
}

type upDownCounter struct {
	sc    syncint64.UpDownCounter
	attrs []attribute.KeyValue
}

func (c *upDownCounter) Inc() {
	c.Add(1)
}

func (c *upDownCounter) Add(incr int) {
	c.sc.Add(context.Background(), int64(incr), c.attrs...)
}

func (c *upDownCounter) Dec() {
	c.Add(-1)
}

func (c *upDownCounter) Sub(diff int) {
	c.Add(-diff)
}

func NewUpDownCounter(name string, description string, unit unit.Unit, labels map[string]any) Counter {
	sc, err := meter.SyncInt64().UpDownCounter(name,
		instrument.WithUnit(unit),
		instrument.WithDescription(description))
	fatalOnErr(err, name)
	return &upDownCounter{
		sc:    sc,
		attrs: getAttrs(labels),
	}
}
