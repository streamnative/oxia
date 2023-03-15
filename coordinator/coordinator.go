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

package coordinator

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/coordinator/impl"
	"oxia/coordinator/model"
	"oxia/kubernetes"
)

type Config struct {
	InternalServiceAddr      string
	MetricsServiceAddr       string
	MetadataProviderImpl     MetadataProviderImpl
	K8SMetadataNamespace     string
	K8SMetadataConfigMapName string
	FileMetadataPath         string
	ClusterConfig            model.ClusterConfig
}

type MetadataProviderImpl string

func (m *MetadataProviderImpl) String() string {
	return string(*m)
}

func (m *MetadataProviderImpl) Set(s string) error {
	switch s {
	case "memory", "configmap", "file":
		*m = MetadataProviderImpl(s)
		return nil
	default:
		return errors.New(`must be one of "memory", "configmap" or "file"`)
	}
}

func (m *MetadataProviderImpl) Type() string {
	return "MetadataProviderImpl"
}

var (
	Memory    MetadataProviderImpl = "memory"
	Configmap MetadataProviderImpl = "configmap"
	File      MetadataProviderImpl = "file"
)

func NewConfig() Config {
	return Config{
		InternalServiceAddr:  fmt.Sprintf("localhost:%d", kubernetes.InternalPort.Port),
		MetricsServiceAddr:   fmt.Sprintf("localhost:%d", kubernetes.MetricsPort.Port),
		MetadataProviderImpl: File,
	}
}

type Coordinator struct {
	coordinator impl.Coordinator
	clientPool  common.ClientPool
	rpcServer   *rpcServer
	metrics     *metrics.PrometheusMetrics
}

func New(config Config) (*Coordinator, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia coordinator")

	s := &Coordinator{
		clientPool: common.NewClientPool(),
	}

	var metadataProvider impl.MetadataProvider
	switch config.MetadataProviderImpl {
	case Memory:
		metadataProvider = impl.NewMetadataProviderMemory()
	case File:
		metadataProvider = impl.NewMetadataProviderFile(config.FileMetadataPath)
	case Configmap:
		k8sConfig := kubernetes.NewClientConfig()
		metadataProvider = impl.NewMetadataProviderConfigMap(kubernetes.NewKubernetesClientset(k8sConfig),
			config.K8SMetadataNamespace, config.K8SMetadataConfigMapName)
	}

	rpcClient := impl.NewRpcProvider(s.clientPool)

	var err error
	if s.coordinator, err = impl.NewCoordinator(metadataProvider, config.ClusterConfig, rpcClient); err != nil {
		return nil, err
	}

	if s.rpcServer, err = newRpcServer(config.InternalServiceAddr); err != nil {
		return nil, err
	}

	if s.metrics, err = metrics.Start(config.MetricsServiceAddr); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Coordinator) Close() error {
	return multierr.Combine(
		s.coordinator.Close(),
		s.rpcServer.Close(),
		s.clientPool.Close(),
		s.metrics.Close(),
	)
}
