package metrics

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
	"io"
	"net"
	"net/http"
	"oxia/common"
)

func init() {
	exporter, err := prometheus.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Prometheus metrics exporter")
	}

	// Use a specific list of buckets for latency histograms
	customBucketsView := metric.NewView(
		metric.Instrument{
			Kind: metric.InstrumentKindSyncHistogram,
			Unit: unit.Milliseconds,
		},
		metric.Stream{
			Aggregation: aggregation.ExplicitBucketHistogram{
				Boundaries: latencyBucketsMillis,
			},
		},
	)

	// Default view to keep all instruments
	defaultView := metric.NewView(metric.Instrument{Name: "*"}, metric.Stream{})

	provider := metric.NewMeterProvider(metric.WithReader(exporter),
		metric.WithView(customBucketsView, defaultView))
	meter = provider.Meter("oxia")

	registry.register()
}

type PrometheusMetrics struct {
	io.Closer

	server *http.Server
	port   int
}

func Start(bindAddress string) (*PrometheusMetrics, error) {
	http.Handle("/metrics", promhttp.Handler())

	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	p := &PrometheusMetrics{
		server: &http.Server{},
		port:   listener.Addr().(*net.TCPAddr).Port,
	}

	log.Info().Msgf("Serving Prometheus metrics at http://localhost:%d/metrics", p.port)

	go common.DoWithLabels(map[string]string{
		"oxia": "metrics",
	}, func() {
		if err = p.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("Failed to serve metrics")
		}
	})

	return p, nil
}

func (p *PrometheusMetrics) Port() int {
	return p.port
}

func (p *PrometheusMetrics) Close() error {
	return p.server.Close()
}
