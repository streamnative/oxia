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
	"log/slog"

	"go.uber.org/multierr"
	"google.golang.org/grpc/health"

	"github.com/streamnative/oxia/server/config"

	"github.com/streamnative/oxia/server/controller"

	"github.com/streamnative/oxia/common/rpc"

	"github.com/streamnative/oxia/common/metric"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
)

type Server struct {
	*internalRpcServer
	*publicRpcServer

	replicationRpcProvider    controller.ReplicationRpcProvider
	shardAssignmentDispatcher ShardAssignmentsDispatcher
	shardsDirector            controller.ShardsDirector
	metrics                   *metric.PrometheusMetrics
	walFactory                wal.Factory
	kvFactory                 kv.Factory

	healthServer *health.Server
}

func New(serverConfig config.ServerConfig) (*Server, error) {
	return NewWithGrpcProvider(serverConfig, rpc.Default, controller.NewReplicationRpcProvider(serverConfig.PeerTLS))
}

func NewWithGrpcProvider(serverConfig config.ServerConfig, provider rpc.GrpcProvider, replicationRpcProvider controller.ReplicationRpcProvider) (*Server, error) {
	slog.Info(
		"Starting Oxia server",
		slog.Any("config", serverConfig),
	)

	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     serverConfig.DataDir,
		CacheSizeMB: serverConfig.DbBlockCacheMB,
	})
	if err != nil {
		return nil, err
	}

	s := &Server{
		replicationRpcProvider: replicationRpcProvider,
		walFactory: wal.NewWalFactory(&wal.FactoryOptions{
			BaseWalDir:  serverConfig.WalDir,
			Retention:   serverConfig.WalRetentionTime,
			SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
			SyncData:    true,
		}),
		kvFactory:    kvFactory,
		healthServer: health.NewServer(),
	}

	s.shardsDirector = controller.NewShardsDirector(serverConfig, s.walFactory, s.kvFactory, replicationRpcProvider)
	s.shardAssignmentDispatcher = NewShardAssignmentDispatcher(s.healthServer)

	s.internalRpcServer, err = newInternalRpcServer(provider, serverConfig.InternalServiceAddr,
		s.shardsDirector, s.shardAssignmentDispatcher, s.healthServer, serverConfig.InternalServerTLS)
	if err != nil {
		return nil, err
	}

	s.publicRpcServer, err = newPublicRpcServer(provider, serverConfig.PublicServiceAddr, s.shardsDirector,
		s.shardAssignmentDispatcher, serverConfig.ServerTLS, &serverConfig.AuthOptions)
	if err != nil {
		return nil, err
	}

	if serverConfig.MetricsServiceAddr != "" {
		s.metrics, err = metric.Start(serverConfig.MetricsServiceAddr)
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
	s.healthServer.Shutdown()

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
