package coordinator

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common"
	"oxia/coordinator/impl"
	"oxia/operator/resource"
	"oxia/server/metrics"
)

type Config struct {
	InternalServicePort int
	MetricsPort         int
	Name                string
	ReplicationFactor   uint32
	ShardCount          uint32
	ServerReplicas      uint32
}

func NewConfig() Config {
	return Config{
		InternalServicePort: resource.InternalPort.Port,
		MetricsPort:         resource.MetricsPort.Port,
		Name:                "oxia",
		ReplicationFactor:   3,
		ShardCount:          3,
		ServerReplicas:      3,
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
	// TODO Eventually this will need a more dynamic method of updating the config when it changes
	// perhaps a controller -> coordinator RPC
	clusterConfig := impl.ClusterConfig{
		ReplicationFactor: config.ReplicationFactor,
		ShardCount:        config.ShardCount,
		StorageServers:    storageServers(config),
	}

	rpcClient := impl.NewRpcProvider(s.clientPool)

	var err error
	if s.coordinator, err = impl.NewCoordinator(metadataProvider, clusterConfig, rpcClient); err != nil {
		return nil, err
	}

	if s.rpcServer, err = NewCoordinatorRpcServer(config.InternalServicePort); err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsPort)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func storageServers(config Config) []impl.ServerAddress {
	servers := make([]impl.ServerAddress, config.ServerReplicas)
	for i := 0; i < int(config.ServerReplicas); i++ {
		servers[i] = impl.ServerAddress{
			Public:   fmt.Sprintf("%s-%d:%d", config.Name, i, resource.PublicPort.Port),
			Internal: fmt.Sprintf("%s-%d:%d", config.Name, i, resource.InternalPort.Port),
		}
	}
	return servers
}

func (s *Coordinator) Close() error {
	return multierr.Combine(
		s.coordinator.Close(),
		s.rpcServer.Close(),
		s.clientPool.Close(),
		s.metrics.Close(),
	)
}
