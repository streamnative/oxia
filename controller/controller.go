package controller

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/server/metrics"
)

type Config struct {
	InternalServicePort int
	MetricsPort         int
}

type Controller struct {
	rpc     *ControllerRpcServer
	metrics *metrics.PrometheusMetrics
}

func New(config Config) (*Controller, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia controller")

	s := &Controller{}

	// TODO add controller behaviour
	// watch for OxiaCluster resources
	// create and manage Oxia clusters

	var err error
	s.rpc, err = NewControllerRpcServer(config.InternalServicePort)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsPort)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Controller) Close() error {
	return multierr.Combine(
		s.rpc.Close(),
		s.metrics.Close(),
	)
}
