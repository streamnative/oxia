package metrics

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
)

func NewGauge(name string, description string, unit unit.Unit, labels map[string]any, callback func() int64) {
	g, err := meter.AsyncInt64().Gauge(name,
		instrument.WithUnit(unit),
		instrument.WithDescription(description),
	)
	fatalOnErr(err, name)

	attrs := getAttrs(labels)

	err = meter.RegisterCallback([]instrument.Asynchronous{g}, func(ctx context.Context) {
		value := callback()
		g.Observe(ctx, value, attrs...)
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to register gauge")
	}
}
