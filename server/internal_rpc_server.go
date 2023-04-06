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
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"oxia/common"
	"oxia/common/container"
	"oxia/proto"
)

type internalRpcServer struct {
	proto.UnimplementedOxiaCoordinationServer
	proto.UnimplementedOxiaLogReplicationServer

	shardsDirector       ShardsDirector
	assignmentDispatcher ShardAssignmentsDispatcher
	grpcServer           container.GrpcServer
	healthServer         *health.Server
	log                  zerolog.Logger
}

func newInternalRpcServer(grpcProvider container.GrpcProvider, bindAddress string, shardsDirector ShardsDirector, assignmentDispatcher ShardAssignmentsDispatcher) (*internalRpcServer, error) {
	server := &internalRpcServer{
		shardsDirector:       shardsDirector,
		assignmentDispatcher: assignmentDispatcher,
		healthServer:         health.NewServer(),
		log: log.With().
			Str("component", "internal-rpc-server").
			Logger(),
	}

	var err error
	server.grpcServer, err = grpcProvider.StartGrpcServer("internal", bindAddress, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaCoordinationServer(registrar, server)
		proto.RegisterOxiaLogReplicationServer(registrar, server)
		grpc_health_v1.RegisterHealthServer(registrar, server.healthServer)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *internalRpcServer) Close() error {
	s.healthServer.Shutdown()
	return s.grpcServer.Close()
}

func (s *internalRpcServer) PushShardAssignments(srv proto.OxiaCoordination_PushShardAssignmentsServer) error {
	s.log.Info().
		Str("peer", common.GetPeer(srv.Context())).
		Msg("Received shard assignment request from coordinator")

	err := s.assignmentDispatcher.PushShardAssignments(srv)
	if err != nil && status.Code(err) != codes.Canceled {
		s.log.Warn().Err(err).
			Str("peer", common.GetPeer(srv.Context())).
			Msg("Failed to provide shards assignments updates")
	}
	return err
}

func (s *internalRpcServer) NewTerm(c context.Context, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	log := s.log.With().
		Interface("req", req).
		Str("peer", common.GetPeer(c)).
		Logger()

	log.Info().Msg("Received NewTerm request")

	// NewTerm applies to both followers and leaders
	// First check if we have already a follower controller running
	if follower, err := s.shardsDirector.GetFollower(req.ShardId); err != nil {
		if status.Code(err) != common.CodeNodeIsNotFollower {
			log.Warn().Err(err).Msg("NewTerm failed: could not get follower controller")
			return nil, err
		}
		log.Debug().Err(err).Msg("Node is not follower, getting leader")

		// If we don't have a follower, fallback to checking the leader controller
	} else {
		log.Info().
			Int64("followerTerm", follower.Term()).
			Msg("Found follower, initiating new term")
		res, err2 := follower.NewTerm(req)
		if err2 != nil {
			log.Warn().Err(err2).Msg("NewTerm of follower failed")
		}
		return res, err
	}

	if leader, err := s.shardsDirector.GetOrCreateLeader(req.Namespace, req.ShardId); err != nil {
		log.Warn().Err(err).Msg("NewTerm failed: could not get leader controller")
		return nil, err
	} else {
		res, err2 := leader.NewTerm(req)
		if err2 != nil {
			log.Warn().Err(err2).Msg("New term processing of leader failed")
		}
		log.Info().
			Interface("response", res).
			Msg("New term processing completed")
		return res, err2
	}
}

func (s *internalRpcServer) BecomeLeader(c context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	log := s.log.With().
		Interface("request", req).
		Str("peer", common.GetPeer(c)).
		Logger()

	log.Info().Msg("Received BecomeLeader request")

	if leader, err := s.shardsDirector.GetOrCreateLeader(req.Namespace, req.ShardId); err != nil {
		log.Warn().Err(err).Msg("BecomeLeader failed: could not get leader controller")
		return nil, err
	} else {
		res, err2 := leader.BecomeLeader(req)
		if err2 != nil {
			log.Warn().Err(err2).Msg("BecomeLeader failed")
		}
		return res, err2
	}
}

func (s *internalRpcServer) AddFollower(c context.Context, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	log := s.log.With().
		Interface("request", req).
		Str("peer", common.GetPeer(c)).
		Logger()

	log.Info().Msg("Received AddFollower request")

	if leader, err := s.shardsDirector.GetLeader(req.ShardId); err != nil {
		log.Warn().Err(err).Msg("AddFollower failed: could not get leader controller")
		return nil, err
	} else {
		res, err2 := leader.AddFollower(req)
		if err2 != nil {
			log.Warn().Err(err2).Msg("AddFollower failed")
		}
		return res, err2
	}
}

func (s *internalRpcServer) Truncate(c context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	log := s.log.With().
		Interface("request", req).
		Str("peer", common.GetPeer(c)).
		Logger()

	log.Info().Msg("Received Truncate request")

	if follower, err := s.shardsDirector.GetOrCreateFollower(req.Namespace, req.ShardId); err != nil {
		log.Warn().Err(err).Msg("Truncate failed: could not get follower controller")
		return nil, err
	} else {
		res, err2 := follower.Truncate(req)

		log.Warn().Err(err2).Msg("Truncate failed")
		return res, err2
	}
}

func (s *internalRpcServer) Replicate(srv proto.OxiaLogReplication_ReplicateServer) error {
	// Add entries receives an incoming stream of request, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return errors.New("shard id is not set in the request metadata")
	}

	shardId, err := ReadHeaderInt64(md, common.MetadataShardId)
	if err != nil {
		return err
	}

	namespace, err := readHeader(md, common.MetadataNamespace)
	if err != nil {
		return err
	}

	log := s.log.With().
		Int64("shard", shardId).
		Str("namespace", namespace).
		Str("peer", common.GetPeer(srv.Context())).
		Logger()

	log.Info().Msg("Received Replicate request")

	if follower, err := s.shardsDirector.GetOrCreateFollower(namespace, shardId); err != nil {
		log.Warn().Err(err).Msg("Replicate failed: could not get follower controller")
		return err
	} else {
		err2 := follower.Replicate(srv)
		if err2 != nil && !errors.Is(err2, io.EOF) {
			log.Warn().Err(err2).Msg("Replicate failed")
		}
		return err2
	}
}

func (s *internalRpcServer) SendSnapshot(srv proto.OxiaLogReplication_SendSnapshotServer) error {
	// Send snapshot receives an incoming stream of requests, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return errors.New("shard id is not set in the request metadata")
	}

	shardId, err := ReadHeaderInt64(md, common.MetadataShardId)
	if err != nil {
		return err
	}

	namespace, err := readHeader(md, common.MetadataNamespace)
	if err != nil {
		return err
	}

	s.log.Info().
		Int64("shard", shardId).
		Str("namespace", namespace).
		Str("peer", common.GetPeer(srv.Context())).
		Msg("Received SendSnapshot request")

	if follower, err := s.shardsDirector.GetOrCreateFollower(namespace, shardId); err != nil {
		s.log.Warn().Err(err).
			Str("namespace", namespace).
			Int64("shard", shardId).
			Str("peer", common.GetPeer(srv.Context())).
			Msg("SendSnapshot failed: could not get follower controller")
		return err
	} else {
		err2 := follower.SendSnapshot(srv)
		if err2 != nil {
			s.log.Warn().Err(err2).
				Str("namespace", namespace).
				Int64("shard", shardId).
				Str("peer", common.GetPeer(srv.Context())).
				Msg("SendSnapshot failed")
		}
		return err2
	}
}

func (s *internalRpcServer) GetStatus(c context.Context, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	if follower, err := s.shardsDirector.GetFollower(req.ShardId); err != nil {
		if status.Code(err) != common.CodeNodeIsNotFollower {
			return nil, err
		}

		// If we don't have a follower, fallback to checking the leader controller
		if leader, err := s.shardsDirector.GetLeader(req.ShardId); err != nil {
			return nil, err
		} else {
			return leader.GetStatus(req)
		}

	} else {
		return follower.GetStatus(req)
	}
}

func (s *internalRpcServer) DeleteShard(c context.Context, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	if follower, err := s.shardsDirector.GetFollower(req.ShardId); err != nil {
		if status.Code(err) != common.CodeNodeIsNotFollower {
			return nil, err
		}

		// If we don't have a follower, fallback to checking the leader controller
		if leader, err := s.shardsDirector.GetLeader(req.ShardId); err != nil {
			return nil, err
		} else {
			return leader.DeleteShard(req)
		}
	} else {
		return follower.DeleteShard(req)
	}
}

func readHeader(md metadata.MD, key string) (value string, err error) {
	arr := md.Get(key)
	if len(arr) == 0 {
		return "", errors.Errorf("Request must include '%s' metadata field", key)
	}

	if len(arr) > 1 {
		return "", errors.Errorf("Request must include '%s' metadata field only once", key)
	}
	return arr[0], nil
}

func ReadHeaderInt64(md metadata.MD, key string) (v int64, err error) {
	s, err := readHeader(md, key)
	if err != nil {
		return 0, err
	}

	var r int64
	_, err = fmt.Sscan(s, &r)
	return r, err
}
