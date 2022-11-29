package metrics

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"io"
	"net"
	"net/http"
	"runtime/pprof"
)

type PrometheusMetrics struct {
	io.Closer

	server *http.Server
	port   int
}

func Start(port int) (*PrometheusMetrics, error) {
	http.Handle("/metrics", promhttp.Handler())

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}

	p := &PrometheusMetrics{
		server: &http.Server{},
		port:   listener.Addr().(*net.TCPAddr).Port,
	}

	log.Info().Msgf("Serving Prometheus metrics at http://localhost:%d/metrics", p.port)

	go pprof.Do(context.Background(),
		pprof.Labels("oxia", "metrics"),
		func(_ context.Context) {
			if err = p.server.Serve(listener); err != nil {
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
