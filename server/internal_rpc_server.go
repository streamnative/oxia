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
			Str("component", "coordination-rpc-server").
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

func (s *internalRpcServer) Fence(c context.Context, req *proto.FenceRequest) (*proto.FenceResponse, error) {
	s.log.Info().
		Interface("req", req).
		Str("peer", common.GetPeer(c)).
		Msg("Received fence request")

	// Fence applies to both followers and leaders
	// First check if we have already a follower controller running
	if follower, err := s.shardsDirector.GetFollower(req.ShardId); err != nil {
		if status.Code(err) != common.CodeNodeIsNotFollower {
			s.log.Warn().Err(err).
				Uint32("shard", req.ShardId).
				Str("peer", common.GetPeer(c)).
				Msg("Fence failed: could not get follower controller")
			return nil, err
		}

		// If we don't have a follower, fallback to checking the leader controller
	} else {
		res, err2 := follower.Fence(req)
		if err2 != nil {
			s.log.Warn().Err(err2).
				Uint32("shard", req.ShardId).
				Str("peer", common.GetPeer(c)).
				Msg("Fence of follower failed")
		}
		return res, err
	}

	if leader, err := s.shardsDirector.GetOrCreateLeader(req.ShardId); err != nil {
		s.log.Warn().Err(err).
			Uint32("shard", req.ShardId).
			Str("peer", common.GetPeer(c)).
			Msg("Fence failed: could not get leader controller")
		return nil, err
	} else {
		res, err2 := leader.Fence(req)
		if err2 != nil {
			s.log.Warn().Err(err2).
				Uint32("shard", req.ShardId).
				Str("peer", common.GetPeer(c)).
				Msg("Fence of leader failed")
		}

		return res, err2
	}
}

func (s *internalRpcServer) BecomeLeader(c context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	s.log.Info().
		Interface("req", req).
		Str("peer", common.GetPeer(c)).
		Msg("Received BecomeLeader request")

	if leader, err := s.shardsDirector.GetOrCreateLeader(req.ShardId); err != nil {
		s.log.Warn().Err(err).
			Uint32("shard", req.ShardId).
			Str("peer", common.GetPeer(c)).
			Msg("BecomeLeader failed: could not get leader controller")
		return nil, err
	} else {
		res, err2 := leader.BecomeLeader(req)
		if err2 != nil {
			s.log.Warn().Err(err2).
				Uint32("shard", req.ShardId).
				Str("peer", common.GetPeer(c)).
				Msg("BecomeLeader failed")
		}
		return res, err2
	}
}

func (s *internalRpcServer) AddFollower(c context.Context, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	s.log.Info().
		Interface("req", req).
		Str("peer", common.GetPeer(c)).
		Msg("Received AddFollower request")

	if leader, err := s.shardsDirector.GetLeader(req.ShardId); err != nil {
		s.log.Warn().Err(err).
			Uint32("shard", req.ShardId).
			Str("peer", common.GetPeer(c)).
			Msg("AddFollower failed: could not get leader controller")
		return nil, err
	} else {
		res, err2 := leader.AddFollower(req)
		if err2 != nil {
			s.log.Warn().Err(err2).
				Uint32("shard", req.ShardId).
				Str("peer", common.GetPeer(c)).
				Msg("AddFollower failed")
		}
		return res, err2
	}
}

func (s *internalRpcServer) Truncate(c context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	s.log.Info().
		Interface("req", req).
		Str("peer", common.GetPeer(c)).
		Msg("Received Truncate request")

	if follower, err := s.shardsDirector.GetOrCreateFollower(req.ShardId); err != nil {
		s.log.Warn().Err(err).
			Uint32("shard", req.ShardId).
			Str("peer", common.GetPeer(c)).
			Msg("Truncate failed: could not get follower controller")
		return nil, err
	} else {
		res, err2 := follower.Truncate(req)

		s.log.Warn().Err(err2).
			Uint32("shard", req.ShardId).
			Str("peer", common.GetPeer(c)).
			Msg("Truncate failed")
		return res, err2
	}
}

func (s *internalRpcServer) AddEntries(srv proto.OxiaLogReplication_ReplicateServer) error {
	// Add entries receives an incoming stream of request, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return errors.New("shard id is not set in the request metadata")
	}

	shardId, err := ReadHeaderUint32(md, common.MetadataShardId)
	if err != nil {
		return err
	}

	s.log.Info().
		Uint32("shard", shardId).
		Str("peer", common.GetPeer(srv.Context())).
		Msg("Received AddEntries request")

	if follower, err := s.shardsDirector.GetOrCreateFollower(shardId); err != nil {
		s.log.Warn().Err(err).
			Uint32("shard", shardId).
			Str("peer", common.GetPeer(srv.Context())).
			Msg("AddEntries failed: could not get follower controller")
		return err
	} else {
		err2 := follower.AddEntries(srv)
		if err2 != nil && !errors.Is(err2, io.EOF) {
			s.log.Warn().Err(err2).
				Uint32("shard", shardId).
				Str("peer", common.GetPeer(srv.Context())).
				Msg("AddEntries failed")
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

	shardId, err := ReadHeaderUint32(md, common.MetadataShardId)
	if err != nil {
		return err
	}

	s.log.Info().
		Uint32("shard", shardId).
		Str("peer", common.GetPeer(srv.Context())).
		Msg("Received SendSnapshot request")

	if follower, err := s.shardsDirector.GetOrCreateFollower(shardId); err != nil {
		s.log.Warn().Err(err).
			Uint32("shard", shardId).
			Str("peer", common.GetPeer(srv.Context())).
			Msg("SendSnapshot failed: could not get follower controller")
		return err
	} else {
		err2 := follower.SendSnapshot(srv)
		if err2 != nil {
			s.log.Warn().Err(err2).
				Uint32("shard", shardId).
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

func ReadHeaderUint32(md metadata.MD, key string) (v uint32, err error) {
	s, err := readHeader(md, key)
	if err != nil {
		return 0, err
	}

	var r uint32
	_, err = fmt.Sscan(s, &r)
	return r, err
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
