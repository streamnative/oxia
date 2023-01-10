package controller

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common/metrics"
	"oxia/kubernetes"
)

type Config struct {
	BindHost            string
	InternalServicePort int
	MetricsPort         int
}

type Controller struct {
	rpc     *ControllerRpcServer
	metrics *metrics.PrometheusMetrics
	watcher Watcher
}

func New(config Config) (*Controller, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia controller")

	s := &Controller{}

	// TODO periodic full bidirectional reconciliation
	// query for resources with label: app.kubernetes.io/managed-by: oxia-controller
	// query for OxiaClusters
	// compare and resolve differences

	conf := kubernetes.NewClientConfig()
	oxia := kubernetes.NewOxiaClientset(conf)
	_kubernetes := kubernetes.NewKubernetesClientset(conf)
	monitoring := kubernetes.NewMonitoringClientset(conf)

	var err error
	s.watcher, err = newWatcher(oxia, newReconciler(_kubernetes, monitoring))
	if err != nil {
		return nil, err
	}

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
		s.watcher.Close(),
	)
}
