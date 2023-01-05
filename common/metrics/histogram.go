package metrics

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
)

var sizeBucketsBytes = []float64{
	0x10, 0x20, 0x40, 0x80,
	0x100, 0x200, 0x400, 0x800,
	0x1000, 0x2000, 0x4000, 0x8000,
	0x10000, 0x20000, 0x40000, 0x80000,
	0x100000, 0x200000, 0x400000, 0x800000,
}

var sizeBucketsCount = []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 10_000, 20_000, 50_000, 100_000, 1_000_000}

type Histogram interface {
	Record(value int)
}

type histogram struct {
	h     syncint64.Histogram
	attrs []attribute.KeyValue
}

func (t *histogram) Record(size int) {
	t.h.Record(context.Background(), int64(size), t.attrs...)
}

func NewCountHistogram(name string, description string, labels map[string]any) Histogram {
	return newHistogram(name, "count", description, labels)
}

func NewBytesHistogram(name string, description string, labels map[string]any) Histogram {
	return newHistogram(name, unit.Bytes, description, labels)
}

func newHistogram(name string, unit unit.Unit, description string, labels map[string]any) Histogram {
	h, err := meter.SyncInt64().Histogram(
		name,
		instrument.WithUnit(unit),
		instrument.WithDescription(description),
	)
	fatalOnErr(err, name)

	return &histogram{h, getAttrs(labels)}
}
