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

type Config struct {
	InternalServicePort int
	PublicServicePort   int
	MetricsPort         int

	AdvertisedPublicAddress string
}

type Server struct {
	*internalRpcServer
	*PublicRpcServer

	shardAssignmentDispatcher ShardAssignmentsDispatcher
	shardsDirector            ShardsDirector
	clientPool                common.ClientPool
	metrics                   *metrics.PrometheusMetrics
	walFactory                wal.WalFactory
	kvFactory                 kv.KVFactory
}

func New(config Config) (*Server, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia server")

	s := &Server{
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
	s.shardAssignmentDispatcher = NewShardAssignmentDispatcher()

	s.internalRpcServer, err = newCoordinationRpcServer(config.InternalServicePort, s.shardsDirector, s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	s.PublicRpcServer, err = NewPublicRpcServer(config.PublicServicePort, s.shardsDirector, s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsPort)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Close() error {
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
