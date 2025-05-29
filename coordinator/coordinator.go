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
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"github.com/streamnative/oxia/common/constant"
	"github.com/streamnative/oxia/common/rpc"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common/metric"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
)

type Config struct {
	InternalServiceAddr              string
	InternalSecureServiceAddr        string
	PeerTLS                          *tls.Config `json:"-"`
	ServerTLS                        *tls.Config `json:"-"`
	MetricsServiceAddr               string
	MetadataProviderImpl             MetadataProviderImpl
	K8SMetadataNamespace             string
	K8SMetadataConfigMapName         string
	FileMetadataPath                 string
	ClusterConfigProvider            func() (model.ClusterConfig, error) `json:"-"`
	ClusterConfigChangeNotifications chan any                            `json:"-"`
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

func (*MetadataProviderImpl) Type() string {
	return "MetadataProviderImpl"
}

var (
	Memory    MetadataProviderImpl = "memory"
	Configmap MetadataProviderImpl = "configmap"
	File      MetadataProviderImpl = "file"
)

func NewConfig() Config {
	return Config{
		InternalServiceAddr:  fmt.Sprintf("localhost:%d", constant.DefaultInternalPort),
		MetricsServiceAddr:   fmt.Sprintf("localhost:%d", constant.DefaultMetricsPort),
		MetadataProviderImpl: File,
	}
}

type Coordinator struct {
	coordinator impl.Coordinator
	clientPool  rpc.ClientPool
	rpcServer   *rpcServer
	metrics     *metric.PrometheusMetrics
}

func New(config Config) (*Coordinator, error) {
	slog.Info(
		"Starting Oxia coordinator",
		slog.Any("config", config),
	)

	s := &Coordinator{
		clientPool: rpc.NewClientPool(config.PeerTLS, nil),
	}

	var metadataProvider impl.MetadataProvider
	switch config.MetadataProviderImpl {
	case Memory:
		metadataProvider = impl.NewMetadataProviderMemory()
	case File:
		metadataProvider = impl.NewMetadataProviderFile(config.FileMetadataPath)
	case Configmap:
		k8sConfig := impl.NewK8SClientConfig()
		metadataProvider = impl.NewMetadataProviderConfigMap(impl.NewK8SClientset(k8sConfig),
			config.K8SMetadataNamespace, config.K8SMetadataConfigMapName)
	}

	rpcClient := impl.NewRpcProvider(s.clientPool)

	var err error
	if s.coordinator, err = impl.NewCoordinator(metadataProvider, config.ClusterConfigProvider, config.ClusterConfigChangeNotifications, rpcClient); err != nil {
		return nil, err
	}

	if s.rpcServer, err = newRpcServer(config.InternalServiceAddr, config.ServerTLS); err != nil {
		return nil, err
	}

	if s.metrics, err = metric.Start(config.MetricsServiceAddr); err != nil {
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
