package standalone

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/common/metrics"
	"oxia/server"
	"oxia/server/kv"
	"oxia/server/wal"
)

type Config struct {
	server.Config

	BindHost string

	AdvertisedPublicAddress string
	NumShards               uint32
	InMemory                bool
}

type Standalone struct {
	rpc        *rpcServer
	kvFactory  kv.KVFactory
	walFactory wal.WalFactory
	metrics    *metrics.PrometheusMetrics
}

func NewTestConfig() Config {
	return Config{
		NumShards:               1,
		BindHost:                "localhost",
		AdvertisedPublicAddress: "localhost",
		InMemory:                true,
	}
}

func New(config Config) (*Standalone, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia standalone")

	s := &Standalone{}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		advertisedPublicAddress = hostname
	}

	var kvOptions kv.KVFactoryOptions
	if config.InMemory {
		kvOptions = kv.KVFactoryOptions{InMemory: true}
		s.walFactory = wal.NewInMemoryWalFactory()
	} else {
		kvOptions = kv.KVFactoryOptions{DataDir: config.DataDir}
		s.walFactory = wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: config.WalDir})
	}
	var err error
	if s.kvFactory, err = kv.NewPebbleKVFactory(&kvOptions); err != nil {
		return nil, err
	}

	s.rpc, err = newRpcServer(config, fmt.Sprintf("%s:%d", config.BindHost, config.PublicServicePort),
		advertisedPublicAddress, config.NumShards, s.walFactory, s.kvFactory)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(fmt.Sprintf("%s:%d", config.BindHost, config.MetricsPort))
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Standalone) RpcPort() int {
	return s.rpc.Port()
}

func (s *Standalone) Close() error {
	return multierr.Combine(
		s.rpc.Close(),
		s.kvFactory.Close(),
		s.metrics.Close(),
	)
}
