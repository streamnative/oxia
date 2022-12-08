package metrics

import (
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	"oxia/proto"
)

func newHistogram(meter metric.Meter, name string, unit unit.Unit) syncint64.Histogram {
	histogram, err := meter.SyncInt64().Histogram(name, instrument.WithUnit(unit))
	fatalOnErr(err, name)
	return histogram
}

func newMillisCounter(meter metric.Meter, name string) syncfloat64.Counter {
	counter, err := meter.SyncFloat64().Counter(name, instrument.WithUnit(unit.Milliseconds))
	fatalOnErr(err, name)
	return counter
}

func newCounter(meter metric.Meter, name string, unit unit.Unit) syncint64.Counter {
	counter, err := meter.SyncInt64().Counter(name, instrument.WithUnit(unit))
	fatalOnErr(err, name)
	return counter
}

func fatalOnErr(err error, name string) {
	if err != nil {
		logger := log.With().Str("component", "oxia-client").Logger()
		logger.Fatal().Err(err).Msgf("Failed to create metric: %s", name)
	}
}

func attrs(requestType string, err error) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Key("type").String(requestType),
		attribute.Key("result").String(result(err)),
	}
}

func result(err error) string {
	if err == nil {
		return "success"
	}
	return "failure"
}

func writeMetrics(request *proto.WriteRequest) (payloadSize int64, requestCount int64) {
	for _, put := range request.Puts {
		payloadSize += int64(len(put.Payload))
	}
	requestCount = int64(len(request.Puts) + len(request.Deletes) + len(request.DeleteRanges))
	return
}

func readMetrics(response *proto.ReadResponse) (payloadSize int64, requestCount int64) {
	if response == nil {
		return
	}
	for _, get := range response.Gets {
		payloadSize += int64(len(get.Payload))
	}
	requestCount = int64(len(response.Gets) + len(response.Lists))
	return
}
