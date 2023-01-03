package metrics

import (
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var meter metric.Meter

func LabelsForShard(shard uint32) map[string]any {
	return map[string]any{
		"shard": shard,
	}
}

func fatalOnErr(err error, name string) {
	if err != nil {
		log.Fatal().Err(err).
			Str("metric-name", name).
			Msg("Failed to create metric")
	}
}

func getAttrs(labels map[string]any) (attrs []attribute.KeyValue) {
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

	return attrs
}
