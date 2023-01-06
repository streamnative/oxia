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
	BindHost                 string
	InternalServicePort      int
	MetricsPort              int
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
	case "memory", "configmap":
		*m = MetadataProviderImpl(s)
		return nil
	default:
		return errors.New(`must be one of "memory" or "configmap"`)
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
		InternalServicePort:  kubernetes.InternalPort.Port,
		MetricsPort:          kubernetes.MetricsPort.Port,
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
		metadataProvider = impl.NewMetadataProviderConfigMap(config.K8SMetadataNamespace, config.K8SMetadataConfigMapName)
	}

	rpcClient := impl.NewRpcProvider(s.clientPool)

	var err error
	if s.coordinator, err = impl.NewCoordinator(metadataProvider, config.ClusterConfig, rpcClient); err != nil {
		return nil, err
	}

	if s.rpcServer, err = newRpcServer(fmt.Sprintf("%s:%d", config.BindHost, config.InternalServicePort)); err != nil {
		return nil, err
	}

	if s.metrics, err = metrics.Start(fmt.Sprintf("%s:%d", config.BindHost, config.MetricsPort)); err != nil {
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
