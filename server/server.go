package server

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/common"
	"oxia/server/kv"
	"oxia/server/wal"
)

type serverConfig struct {
	InternalServicePort int
	PublicServicePort   int

	AdvertisedInternalAddress string
	AdvertisedPublicAddress   string
}

type server struct {
	*internalRpcServer
	*PublicRpcServer

	shardsDirector ShardsDirector
	clientPool     common.ClientPool
	walFactory     wal.WalFactory
	kvFactory      kv.KVFactory
}

func NewServer(config *serverConfig) (*server, error) {
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
	)
}
