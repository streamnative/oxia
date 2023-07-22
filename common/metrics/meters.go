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
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var meter metric.Meter

func LabelsForShard(namespace string, shard int64) map[string]any {
	return map[string]any{
		"shard":          shard,
		"oxia_namespace": namespace,
	}
}

func fatalOnErr(err error, name string) {
	if err != nil {
		log.Fatal().Err(err).
			Str("metric-name", name).
			Msg("Failed to create metric")
	}
}

func getAttrs(labels map[string]any) (options metric.MeasurementOption) {
	attrs := make([]attribute.KeyValue, 0)
	for k, v := range labels {
		key := attribute.Key(k)
		var attr attribute.KeyValue
		switch t := v.(type) {
		case uint32:
			attr = key.Int64(int64(t))
		case int64:
			attr = key.Int64(t)
		case int:
			attr = key.Int(t)
		case float64:
			attr = key.Float64(t)
		case bool:
			attr = key.Bool(t)
		case string:
			attr = key.String(t)

		default:
			log.Fatal().Msgf("Invalid label type %#v", v)
		}

		attrs = append(attrs, attr)
	}

	return metric.WithAttributes(attrs...)
}
