package server

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common/container"
	"oxia/common/metrics"
	"oxia/server/kv"
	"oxia/server/wal"
	"time"
)

type Config struct {
	BindHost            string
	PublicServicePort   int
	InternalServicePort int
	MetricsPort         int
	DataDir             string
	WalDir              string
	WalRetentionTime    time.Duration
}

type Server struct {
	*internalRpcServer
	*PublicRpcServer

	replicationRpcProvider    ReplicationRpcProvider
	shardAssignmentDispatcher ShardAssignmentsDispatcher
	shardsDirector            ShardsDirector
	metrics                   *metrics.PrometheusMetrics
	walFactory                wal.WalFactory
	kvFactory                 kv.KVFactory
}

func New(config Config) (*Server, error) {
	return NewWithGrpcProvider(config, container.Default, NewReplicationRpcProvider())
}

func NewWithGrpcProvider(config Config, provider container.GrpcProvider, replicationRpcProvider ReplicationRpcProvider) (*Server, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia server")

	kvFactory, err := kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir:   config.DataDir,
		CacheSize: 100 * 1024 * 1024,
	})
	if err != nil {
		return nil, err
	}

	s := &Server{
		replicationRpcProvider: replicationRpcProvider,
		walFactory: wal.NewWalFactory(&wal.WalFactoryOptions{
			LogDir: config.WalDir,
		}),
		kvFactory: kvFactory,
	}

	s.shardsDirector = NewShardsDirector(config, s.walFactory, s.kvFactory, replicationRpcProvider)
	s.shardAssignmentDispatcher = NewShardAssignmentDispatcher()

	s.internalRpcServer, err = newCoordinationRpcServer(provider,
		fmt.Sprintf("%s:%d", config.BindHost, config.InternalServicePort),
		s.shardsDirector, s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	s.PublicRpcServer, err = NewPublicRpcServer(provider,
		fmt.Sprintf("%s:%d", config.BindHost, config.PublicServicePort), s.shardsDirector, s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	if config.MetricsPort >= 0 {
		s.metrics, err = metrics.Start(fmt.Sprintf("%s:%d", config.BindHost, config.MetricsPort))
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) PublicPort() int {
	return s.PublicRpcServer.grpcServer.Port()
}

func (s *Server) InternalPort() int {
	return s.internalRpcServer.grpcServer.Port()
}

func (s *Server) Close() error {
	err := multierr.Combine(
		s.shardAssignmentDispatcher.Close(),
		s.shardsDirector.Close(),
		s.PublicRpcServer.Close(),
		s.internalRpcServer.Close(),
		s.kvFactory.Close(),
		s.walFactory.Close(),
		s.replicationRpcProvider.Close(),
	)

	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}

	return err
}
