package server

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/common"
	"oxia/server/kv"
	"oxia/server/metrics"
	"oxia/server/wal"
)

type serverConfig struct {
	InternalServicePort int
	PublicServicePort   int
	MetricsPort         int

	AdvertisedInternalAddress string
	AdvertisedPublicAddress   string
}

type server struct {
	*internalRpcServer
	*PublicRpcServer

	shardsDirector ShardsDirector
	clientPool     common.ClientPool
	metrics        *metrics.PrometheusMetrics
	walFactory     wal.WalFactory
	kvFactory      kv.KVFactory
}

func newServer(config serverConfig) (*server, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia server")

	s := &server{
		clientPool: common.NewClientPool(),
		walFactory: wal.NewWalFactory(nil),
		kvFactory:  kv.NewPebbleKVFactory(nil),
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	advertisedInternalAddress := config.AdvertisedInternalAddress
	if advertisedInternalAddress == "" {
		advertisedInternalAddress = hostname
	}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		advertisedPublicAddress = hostname
	}
	log.Info().Msgf("AdvertisedPublicAddress %s", advertisedPublicAddress)

	s.shardsDirector = NewShardsDirector(s.walFactory, s.kvFactory)

	s.internalRpcServer, err = newCoordinationRpcServer(config.InternalServicePort, s.shardsDirector)
	if err != nil {
		return nil, err
	}

	s.PublicRpcServer, err = NewPublicRpcServer(config.PublicServicePort, s.shardsDirector)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsPort)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Close() error {
	return multierr.Combine(
		s.PublicRpcServer.Close(),
		s.internalRpcServer.Close(),
		s.clientPool.Close(),
		s.shardsDirector.Close(),
		s.kvFactory.Close(),
		s.walFactory.Close(),
		s.metrics.Close(),
	)
}
