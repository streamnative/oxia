package metrics

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
)

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
