// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"oxia/common/container"
	"oxia/common/metrics"
	"oxia/server/kv"
	"oxia/server/wal"
	"time"
)

type Config struct {
	PublicServiceAddr   string
	InternalServiceAddr string
	MetricsServiceAddr  string
	DataDir             string
	WalDir              string

	WalRetentionTime           time.Duration
	NotificationsRetentionTime time.Duration
}

type Server struct {
	*internalRpcServer
	*publicRpcServer

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

	s.internalRpcServer, err = newInternalRpcServer(provider, config.InternalServiceAddr,
		s.shardsDirector, s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	s.publicRpcServer, err = newPublicRpcServer(provider, config.PublicServiceAddr, s.shardsDirector,
		s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	if config.MetricsServiceAddr != "" {
		s.metrics, err = metrics.Start(config.MetricsServiceAddr)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) PublicPort() int {
	return s.publicRpcServer.grpcServer.Port()
}

func (s *Server) InternalPort() int {
	return s.internalRpcServer.grpcServer.Port()
}

func (s *Server) Close() error {
	err := multierr.Combine(
		s.shardAssignmentDispatcher.Close(),
		s.shardsDirector.Close(),
		s.publicRpcServer.Close(),
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
