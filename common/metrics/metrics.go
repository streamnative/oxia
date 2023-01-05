package metrics

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"io"
	"net"
	"net/http"
	"oxia/common"
)

type PrometheusMetrics struct {
	io.Closer

	server *http.Server
	port   int
}

func Start(bindAddress string) (*PrometheusMetrics, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	p := &PrometheusMetrics{
		server: &http.Server{Handler: mux},
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
