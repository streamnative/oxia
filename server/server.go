package server

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common"
	"oxia/server/kv"
	"oxia/server/metrics"
	"oxia/server/wal"
)

type serverConfig struct {
	InternalServicePort int
	PublicServicePort   int
	MetricsPort         int
	DataDir             string
	WalDir              string
}

type server struct {
	*internalRpcServer
	*PublicRpcServer

	shardAssignmentDispatcher ShardAssignmentsDispatcher
	shardsDirector            ShardsDirector
	clientPool                common.ClientPool
	metrics                   *metrics.PrometheusMetrics
	walFactory                wal.WalFactory
	kvFactory                 kv.KVFactory
}

func newServer(config serverConfig) (*server, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia server")

	s := &server{
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
