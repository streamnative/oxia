package metrics

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"time"
)

type Timer interface {
	Record(ctx context.Context, incr time.Duration, attrs ...attribute.KeyValue)
}

type timerImpl struct {
	sum   syncfloat64.Counter
	count syncint64.Counter
}

func newTimer(meter metric.Meter, name string) Timer {
	return &timerImpl{
		sum:   newMillisCounter(meter, name),
		count: newCounter(meter, name, ""),
	}
}

func (t *timerImpl) Record(ctx context.Context, incr time.Duration, attrs ...attribute.KeyValue) {
	millis := float64(incr) / float64(time.Millisecond)
	t.sum.Add(ctx, millis, attrs...)
	t.count.Add(ctx, 1, attrs...)
}
