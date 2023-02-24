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
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"oxia/common"
	"oxia/common/container"
	"oxia/proto"
)

const (
	maxTotalReadValueSize = 4 << (10 * 2) //4Mi
	maxTotalListKeySize   = 4 << (10 * 2) //4Mi
)

type publicRpcServer struct {
	proto.UnimplementedOxiaClientServer

	shardsDirector       ShardsDirector
	assignmentDispatcher ShardAssignmentsDispatcher
	grpcServer           container.GrpcServer
	log                  zerolog.Logger
}

func newPublicRpcServer(provider container.GrpcProvider, bindAddress string, shardsDirector ShardsDirector, assignmentDispatcher ShardAssignmentsDispatcher) (*publicRpcServer, error) {
	server := &publicRpcServer{
		shardsDirector:       shardsDirector,
		assignmentDispatcher: assignmentDispatcher,
		log: log.With().
			Str("component", "public-rpc-server").
			Logger(),
	}

	var err error
	server.grpcServer, err = provider.StartGrpcServer("public", bindAddress, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaClientServer(registrar, server)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *publicRpcServer) GetShardAssignments(_ *proto.ShardAssignmentsRequest, srv proto.OxiaClient_GetShardAssignmentsServer) error {
	s.log.Debug().
		Str("peer", common.GetPeer(srv.Context())).
		Msg("Shard assignments requests")
	err := s.assignmentDispatcher.RegisterForUpdates(srv)
	if err != nil {
		s.log.Warn().Err(err).
			Str("peer", common.GetPeer(srv.Context())).
			Msg("Failed to add client for shards assignments notifications")
	}

	return err
}

func (s *publicRpcServer) Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", write).
		Msg("Write request")

	lc, err := s.getLeader(*write.ShardId)
	if err != nil {
		return nil, err
	}

	wr, err := lc.Write(write)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to perform write operation")
	}

	return wr, err
}

func (s *publicRpcServer) Read(request *proto.ReadRequest, stream proto.OxiaClient_ReadServer) error {
	s.log.Debug().
		Str("peer", common.GetPeer(stream.Context())).
		Interface("req", request).
		Msg("Read request")

	lc, err := s.getLeader(*request.ShardId)
	if err != nil {
		return err
	}

	ch := lc.Read(stream.Context(), request)

	response := &proto.ReadResponse{}
	var totalSize int

	for {
		select {
		case result, more := <-ch:
			if !more {
				if len(response.Gets) > 0 {
					if err := stream.Send(response); err != nil {
						return err
					}
				}
				return nil
			}
			if result.Err != nil {
				return result.Err
			}
			size := protowire.SizeBytes(len(result.Response.Value))
			if len(response.Gets) > 0 && totalSize+size > maxTotalReadValueSize {
				if err := stream.Send(response); err != nil {
					return err
				}
				response = &proto.ReadResponse{}
				totalSize = 0
			}
			response.Gets = append(response.Gets, result.Response)
			totalSize += size
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *publicRpcServer) List(request *proto.ListRequest, stream proto.OxiaClient_ListServer) error {
	s.log.Debug().
		Str("peer", common.GetPeer(stream.Context())).
		Interface("req", request).
		Msg("List request")

	lc, err := s.getLeader(*request.ShardId)
	if err != nil {
		return err
	}

	ch, err := lc.List(stream.Context(), request)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to perform list operation")
	}

	response := &proto.ListResponse{}
	var totalSize int

	for {
		select {
		case key, more := <-ch:
			if !more {
				if len(response.Keys) > 0 {
					if err := stream.Send(response); err != nil {
						return err
					}
				}
				return nil
			}
			size := protowire.SizeBytes(len(key))
			if len(response.Keys) > 0 && totalSize+size > maxTotalListKeySize {
				if err := stream.Send(response); err != nil {
					return err
				}
				response = &proto.ListResponse{}
				totalSize = 0
			}
			response.Keys = append(response.Keys, key)
			totalSize += size
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *publicRpcServer) GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	s.log.Debug().
		Str("peer", common.GetPeer(stream.Context())).
		Interface("req", req).
		Msg("Get notifications")

	lc, err := s.getLeader(req.ShardId)
	if err != nil {
		return err
	}

	if err = lc.GetNotifications(req, stream); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn().Err(err).
			Msg("Failed to handle notifications request")
	}

	return err
}

func (s *publicRpcServer) Port() int {
	return s.grpcServer.Port()
}

func (s *publicRpcServer) CreateSession(ctx context.Context, req *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", req).
		Msg("Create session request")
	lc, err := s.getLeader(req.ShardId)
	if err != nil {
		return nil, err
	}
	res, err := lc.CreateSession(req)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to create session")
		return nil, err
	}
	return res, nil
}

func (s *publicRpcServer) KeepAlive(ctx context.Context, req *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	s.log.Debug().
		Uint32("shard", req.ShardId).
		Int64("session", req.SessionId).
		Str("peer", common.GetPeer(ctx)).
		Msg("Session keep alive")
	lc, err := s.getLeader(req.ShardId)
	if err != nil {
		return nil, err
	}
	err = lc.KeepAlive(req.SessionId)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to listen to heartbeats")
		return nil, err
	}
	return &proto.KeepAliveResponse{}, nil
}

func (s *publicRpcServer) CloseSession(ctx context.Context, req *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", req).
		Msg("Close session request")
	lc, err := s.getLeader(req.ShardId)
	if err != nil {
		return nil, err
	}
	res, err := lc.CloseSession(req)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to close session")
		return nil, err
	}
	return res, nil
}

func (s *publicRpcServer) getLeader(shardId uint32) (LeaderController, error) {
	lc, err := s.shardsDirector.GetLeader(shardId)
	if err != nil {
		if status.Code(err) != common.CodeNodeIsNotLeader {
			s.log.Warn().Err(err).
				Msg("Failed to get the leader controller")
		}
		return nil, err
	}
	return lc, nil
}

func (s *publicRpcServer) Close() error {
	return s.grpcServer.Close()
}
