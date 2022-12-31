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
}

type Standalone struct {
	rpc        *StandaloneRpcServer
	kvFactory  kv.KVFactory
	walFactory wal.WalFactory
	metrics    *metrics.PrometheusMetrics
}

func New(config Config) (*Standalone, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia standalone")

	s := &Standalone{}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		advertisedPublicAddress = hostname
	}

	s.walFactory = wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: config.WalDir,
	})

	if s.kvFactory, err = kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir: config.DataDir,
	}); err != nil {
		return nil, err
	}

	s.rpc, err = NewStandaloneRpcServer(config, fmt.Sprintf("%s:%d", config.BindHost, config.PublicServicePort), advertisedPublicAddress, config.NumShards, s.walFactory, s.kvFactory)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(fmt.Sprintf("%s:%d", config.BindHost, config.MetricsPort))
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Standalone) Close() error {
	return multierr.Combine(
		s.rpc.Close(),
		s.kvFactory.Close(),
		s.metrics.Close(),
	)
}
