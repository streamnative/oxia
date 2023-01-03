package controller

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common/metrics"
)

type Config struct {
	BindHost            string
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
	s.rpc, err = NewControllerRpcServer(fmt.Sprintf("%s:%d", config.BindHost, config.InternalServicePort))
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(fmt.Sprintf("%s:%d", config.BindHost, config.MetricsPort))
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
