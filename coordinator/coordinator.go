package coordinator

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/coordinator/impl"
	"oxia/kubernetes"
	"oxia/server/metrics"
)

type Config struct {
	BindHost            string
	InternalServicePort int
	MetricsPort         int
	ClusterConfig       impl.ClusterConfig
}

func NewConfig() Config {
	return Config{
		InternalServicePort: kubernetes.InternalPort.Port,
		MetricsPort:         kubernetes.MetricsPort.Port,
	}
}

type Coordinator struct {
	coordinator impl.Coordinator
	clientPool  common.ClientPool
	rpcServer   *CoordinatorRpcServer
	metrics     *metrics.PrometheusMetrics
}

func New(config Config) (*Coordinator, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia coordinator")

	s := &Coordinator{
		clientPool: common.NewClientPool(),
	}

	metadataProvider := impl.NewMetadataProviderMemory()

	rpcClient := impl.NewRpcProvider(s.clientPool)

	var err error
	if s.coordinator, err = impl.NewCoordinator(metadataProvider, config.ClusterConfig, rpcClient); err != nil {
		return nil, err
	}

	if s.rpcServer, err = NewCoordinatorRpcServer(fmt.Sprintf("%s:%d", config.BindHost, config.InternalServicePort)); err != nil {
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
