package coordinator

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/coordinator/impl"
	"oxia/operator/resource"
	"oxia/server/metrics"
)

type Config struct {
	InternalServicePort int
	MetricsPort         int
	Name                string
	ReplicationFactor   uint32
	ShardsCount         uint32
	ServerReplicas      uint32
}

func NewConfig() Config {
	return Config{
		InternalServicePort: resource.InternalPort.Port,
		MetricsPort:         resource.MetricsPort.Port,
		Name:                "oxia",
		ReplicationFactor:   3,
		ShardsCount:         3,
		ServerReplicas:      3,
	}
}

type Coordinator struct {
	coordinator impl.Coordinator
	rpc         *CoordinatorRpcServer
	metrics     *metrics.PrometheusMetrics
}

func New(config Config) (*Coordinator, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia coordinator")

	s := &Coordinator{}

	metadataProvider := impl.NewMetadataProviderMemory()
	// TODO Eventually this will need a more dynamic method of updating the config when it changes
	// perhaps a controller -> coordinator RPC
	clusterConfig := impl.ClusterConfig{
		ReplicationFactor: config.ReplicationFactor,
		ShardsCount:       config.ShardsCount,
		StorageServers:    storageServers(config),
	}

	var err error
	s.coordinator, err = impl.NewCoordinator(metadataProvider, clusterConfig)
	if err != nil {
		return nil, err
	}

	s.rpc, err = NewCoordinatorRpcServer(config.InternalServicePort)
	if err != nil {
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
		s.rpc.Close(),
		s.metrics.Close(),
	)
}
