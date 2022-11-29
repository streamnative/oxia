package standalone

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/server/kv"
	"oxia/server/metrics"
)

type standaloneConfig struct {
	PublicServicePort uint32
	MetricsPort       int

	AdvertisedPublicAddress string

	NumShards uint32
	DataDir   string
}

type standalone struct {
	rpc       *StandaloneRpcServer
	kvFactory kv.KVFactory
	metrics   *metrics.PrometheusMetrics
}

func newStandalone(config *standaloneConfig) (*standalone, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia standalone")

	s := &standalone{}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		advertisedPublicAddress = hostname
	}

	identityAddr := fmt.Sprintf("%s:%d", advertisedPublicAddress, config.PublicServicePort)

	s.kvFactory = kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir: config.DataDir,
	})

	s.rpc, err = NewStandaloneRpcServer(int(config.PublicServicePort), identityAddr, config.NumShards, s.kvFactory)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsPort)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *standalone) Close() error {
	var errs error
	if err := s.rpc.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := s.kvFactory.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := s.metrics.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	return nil
}
