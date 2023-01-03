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
		switch v.(type) {
		case uint32:
			attr = key.Int64(int64(v.(uint32)))
		case int64:
			attr = key.Int64(v.(int64))
		case int:
			attr = key.Int(v.(int))
		case float64:
			attr = key.Float64(v.(float64))
		case bool:
			attr = key.Bool(v.(bool))
		case string:
			attr = key.String(v.(string))

		default:
			log.Fatal().Msgf("Invalid label type %#v", v)
		}

		attrs = append(attrs, attr)
	}

	return attrs
}
