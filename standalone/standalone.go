package standalone

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/server/kv"
	"oxia/server/metrics"
	"oxia/server/wal"
)

type Config struct {
	PublicServicePort int
	MetricsPort       int

	AdvertisedPublicAddress string
	NumShards               uint32
	DataDir                 string
	WalDir                  string
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

	s.rpc, err = NewStandaloneRpcServer(config.PublicServicePort, advertisedPublicAddress, config.NumShards, s.walFactory, s.kvFactory)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsPort)
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
