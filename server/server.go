package server

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common"
	"oxia/server/kv"
	"oxia/server/metrics"
	"oxia/server/wal"
)

type Config struct {
	PublicServicePort   int
	InternalServicePort int
	MetricsPort         int
	DataDir             string
	WalDir              string
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
		walFactory: wal.NewWalFactory(&wal.WalFactoryOptions{
			LogDir: config.WalDir,
		}),
		kvFactory: kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
			DataDir:   config.DataDir,
			CacheSize: 100 * 1024 * 1024,
		}),
	}

	s.shardsDirector = NewShardsDirector(s.walFactory, s.kvFactory)
	s.shardAssignmentDispatcher = NewShardAssignmentDispatcher()

	var err error
	s.internalRpcServer, err = newCoordinationRpcServer(config.InternalServicePort, s.shardsDirector, s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	s.PublicRpcServer, err = NewPublicRpcServer(config.PublicServicePort, s.shardsDirector, s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	if config.MetricsPort >= 0 {
		s.metrics, err = metrics.Start(config.MetricsPort)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) PublicPort() int {
	return s.PublicRpcServer.container.Port()
}

func (s *Server) InternalPort() int {
	return s.internalRpcServer.container.Port()
}

func (s *Server) Close() error {
	err := multierr.Combine(
		s.shardAssignmentDispatcher.Close(),
		s.shardsDirector.Close(),
		s.PublicRpcServer.Close(),
		s.internalRpcServer.Close(),
		s.clientPool.Close(),
		s.kvFactory.Close(),
		s.walFactory.Close(),
	)

	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}

	return err
}
