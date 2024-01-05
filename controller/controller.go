// Copyright 2024 StreamNative, Inc.
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

package controller

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common/metrics"
	"oxia/kubernetes"
)

type Config struct {
	InternalServiceAddr string
	MetricsServiceAddr  string
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

	s.rpc, err = NewControllerRpcServer(config.InternalServiceAddr)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsServiceAddr)
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
