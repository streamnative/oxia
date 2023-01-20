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

func writeMetrics(request *proto.WriteRequest) (valueSize int64, requestCount int64) {
	for _, put := range request.Puts {
		valueSize += int64(len(put.Value))
	}
	requestCount = int64(len(request.Puts) + len(request.Deletes) + len(request.DeleteRanges))
	return
}

func readMetrics(response *proto.ReadResponse) (valueSize int64, requestCount int64) {
	if response == nil {
		return
	}
	for _, get := range response.Gets {
		valueSize += int64(len(get.Value))
	}
	requestCount = int64(len(response.Gets) + len(response.Lists))
	return
}
