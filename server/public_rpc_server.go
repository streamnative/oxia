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
	"crypto/tls"
	"io"
	"log/slog"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/streamnative/oxia/common/callback"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/container"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/auth"
)

const (
	maxTotalScanBatchCount = 1000
	maxTotalListKeyCount   = 0             // no limitation
	maxTotalListKeySize    = 2 << (10 * 2) // 2Mi
	maxTotalReadCount      = 0
	maxTotalReadValueSize  = 2 << (10 * 2) // 2Mi
)

type publicRpcServer struct {
	proto.UnimplementedOxiaClientServer

	shardsDirector       ShardsDirector
	assignmentDispatcher ShardAssignmentsDispatcher
	grpcServer           container.GrpcServer
	log                  *slog.Logger
}

func newPublicRpcServer(provider container.GrpcProvider, bindAddress string, shardsDirector ShardsDirector, assignmentDispatcher ShardAssignmentsDispatcher,
	tlsConf *tls.Config, options *auth.Options) (*publicRpcServer, error) {
	server := &publicRpcServer{
		shardsDirector:       shardsDirector,
		assignmentDispatcher: assignmentDispatcher,
		log: slog.With(
			slog.String("component", "public-rpc-server"),
		),
	}

	var err error
	server.grpcServer, err = provider.StartGrpcServer("public", bindAddress, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaClientServer(registrar, server)
	}, tlsConf, options)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *publicRpcServer) GetShardAssignments(req *proto.ShardAssignmentsRequest, srv proto.OxiaClient_GetShardAssignmentsServer) error {
	s.log.Debug(
		"Shard assignments requests",
		slog.String("peer", common.GetPeer(srv.Context())),
	)
	err := s.assignmentDispatcher.RegisterForUpdates(req, srv)
	if err != nil {
		s.log.Warn(
			"Failed to add client for shards assignments notifications",
			slog.Any("error", err),
			slog.String("peer", common.GetPeer(srv.Context())),
		)
		return err
	}

	return err
}

func (s *publicRpcServer) Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
	s.log.Debug(
		"Write request",
		slog.String("peer", common.GetPeer(ctx)),
		slog.Any("req", write),
	)

	lc, err := s.getLeader(*write.Shard)
	if err != nil {
		return nil, err
	}

	wr, err := lc.Write(ctx, write)
	if err != nil {
		s.log.Warn(
			"Failed to perform write operation",
			slog.Any("error", err),
		)
		return nil, err
	}

	return wr, err
}

func (s *publicRpcServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	// Add entries receives an incoming stream of request, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(stream.Context())
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

	log := s.log.With(
		slog.Int64("shard", shardId),
		slog.String("namespace", namespace),
		slog.String("peer", common.GetPeer(stream.Context())),
	)

	log.Debug("Write Stream request")

	lc, err := s.getLeader(shardId)
	if err != nil {
		return err
	}

	if err = lc.WriteStream(stream); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			log.Info("Write stream has been closed")
			return nil
		}
		if errors.Is(err, ErrLeaderClosed) {
			log.Info("Write stream has been closed by leader closed")
			return err
		}
		log.Warn("Write stream has been closed by error", slog.Any("error", err))
		return err
	}
	return err
}

func (s *publicRpcServer) Read(request *proto.ReadRequest, stream proto.OxiaClient_ReadServer) error {
	s.log.Debug(
		"Read request",
		slog.String("peer", common.GetPeer(stream.Context())),
		slog.Any("req", request),
	)

	if request.Shard == nil {
		return status.Error(codes.InvalidArgument, "shard id is required")
	}

	lc, err := s.getLeader(*request.Shard)
	if err != nil {
		return err
	}

	ctx := stream.Context()

	finish := make(chan error, 1)
	lc.Read(stream.Context(), request, callback.NewBatchStreamOnce[*proto.GetResponse](maxTotalReadCount, maxTotalReadValueSize,
		func(result *proto.GetResponse) int { return protowire.SizeBytes(len(result.Value)) },
		func(container []*proto.GetResponse) error { return stream.Send(&proto.ReadResponse{Gets: container}) },
		func(err error) { finish <- err },
	))

	for {
		select {
		case err = <-finish:
			if err != nil {
				s.log.Warn(
					"Failed to perform list operation",
					slog.Any("error", err),
				)
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *publicRpcServer) List(request *proto.ListRequest, stream proto.OxiaClient_ListServer) error {
	s.log.Debug(
		"List request",
		slog.String("peer", common.GetPeer(stream.Context())),
		slog.Any("req", request),
	)
	if request.Shard == nil {
		return status.Error(codes.InvalidArgument, "shard id is required")
	}
	lc, err := s.getLeader(*request.Shard)
	if err != nil {
		return err
	}
	ctx := stream.Context()
	finish := make(chan error, 1)
	lc.List(ctx, request, callback.NewBatchStreamOnce[string](maxTotalListKeyCount, maxTotalListKeySize,
		func(key string) int { return protowire.SizeBytes(len(key)) },
		func(container []string) error { return stream.Send(&proto.ListResponse{Keys: container}) },
		func(err error) { finish <- err },
	))
	for {
		select {
		case err = <-finish:
			if err != nil {
				s.log.Warn(
					"Failed to perform list operation",
					slog.Any("error", err),
				)
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *publicRpcServer) RangeScan(request *proto.RangeScanRequest, stream proto.OxiaClient_RangeScanServer) error {
	s.log.Debug(
		"RangeScan request",
		slog.String("peer", common.GetPeer(stream.Context())),
		slog.Any("req", request),
	)
	if request.Shard == nil {
		return status.Error(codes.InvalidArgument, "shard id is required")
	}
	ctx := stream.Context()

	var lc LeaderController
	var err error
	if lc, err = s.getLeader(*request.Shard); err != nil {
		return err
	}

	finish := make(chan error, 1)
	lc.RangeScan(ctx, request,
		callback.NewBatchStreamOnce[*proto.GetResponse](maxTotalScanBatchCount, maxTotalReadValueSize,
			func(response *proto.GetResponse) int { return len(response.Value) },
			func(container []*proto.GetResponse) error {
				return stream.Send(&proto.RangeScanResponse{Records: container})
			},
			func(err error) {
				finish <- err
				close(finish)
			}),
	)

	for {
		select {
		case err := <-finish:
			if err != nil {
				s.log.Warn(
					"Failed to perform range-scan operation",
					slog.Any("error", err),
				)
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *publicRpcServer) GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	s.log.Debug(
		"Get notifications",
		slog.String("peer", common.GetPeer(stream.Context())),
		slog.Any("req", req),
	)

	lc, err := s.getLeader(req.Shard)
	if err != nil {
		return err
	}

	if err = lc.GetNotifications(req, stream); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn(
			"Failed to handle notifications request",
			slog.Any("error", err),
		)
	}

	return err
}

func (s *publicRpcServer) Port() int {
	return s.grpcServer.Port()
}

func (s *publicRpcServer) CreateSession(ctx context.Context, req *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	s.log.Debug(
		"Create session request",
		slog.String("peer", common.GetPeer(ctx)),
		slog.Any("req", req),
	)
	lc, err := s.getLeader(req.Shard)
	if err != nil {
		return nil, err
	}
	res, err := lc.CreateSession(req)
	if err != nil {
		s.log.Warn(
			"Failed to create session",
			slog.Any("error", err),
		)
		return nil, err
	}
	return res, nil
}

func (s *publicRpcServer) KeepAlive(ctx context.Context, req *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	s.log.Debug(
		"Session keep alive",
		slog.Int64("shard", req.Shard),
		slog.Int64("session", req.SessionId),
		slog.String("peer", common.GetPeer(ctx)),
	)
	lc, err := s.getLeader(req.Shard)
	if err != nil {
		return nil, err
	}
	err = lc.KeepAlive(req.SessionId)
	if err != nil {
		s.log.Warn(
			"Failed to listen to heartbeats",
			slog.Any("error", err),
		)
		return nil, err
	}
	return &proto.KeepAliveResponse{}, nil
}

func (s *publicRpcServer) CloseSession(ctx context.Context, req *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	s.log.Debug(
		"Close session request",
		slog.String("peer", common.GetPeer(ctx)),
		slog.Any("req", req),
	)
	lc, err := s.getLeader(req.Shard)
	if err != nil {
		return nil, err
	}
	res, err := lc.CloseSession(req)
	if err != nil {
		if status.Code(err) != common.CodeSessionNotFound {
			s.log.Warn(
				"Failed to close session",
				slog.Any("error", err),
			)
			return nil, err
		}

		s.log.Warn("Session not found, it should already closed")
	}
	return res, nil
}

func (s *publicRpcServer) GetSequenceUpdates(req *proto.GetSequenceUpdatesRequest,
	stream proto.OxiaClient_GetSequenceUpdatesServer) error {
	s.log.Debug(
		"Get sequence update request",
		slog.String("peer", common.GetPeer(stream.Context())),
		slog.Any("req", req),
	)
	lc, err := s.getLeader(req.Shard)
	if err != nil {
		return err
	}

	return lc.GetSequenceUpdates(req, stream)
}

func (s *publicRpcServer) getLeader(shardId int64) (LeaderController, error) {
	lc, err := s.shardsDirector.GetLeader(shardId)
	if err != nil {
		if status.Code(err) != common.CodeNodeIsNotLeader {
			s.log.Warn(
				"Failed to get the leader controller",
				slog.Any("error", err),
			)
		}
		return nil, err
	}
	return lc, nil
}

func (s *publicRpcServer) Close() error {
	return s.grpcServer.Close()
}
