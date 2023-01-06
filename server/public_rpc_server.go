package server

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"oxia/common"
	"oxia/common/container"
	"oxia/proto"
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

func (s *publicRpcServer) ShardAssignments(_ *proto.ShardAssignmentsRequest, srv proto.OxiaClient_ShardAssignmentsServer) error {
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

	lc, err := s.shardsDirector.GetLeader(*write.ShardId)
	if err != nil {
		if status.Code(err) != common.CodeNodeIsNotLeader {
			s.log.Warn().Err(err).
				Msg("Failed to get the leader controller")
		}
		return nil, err
	}

	wr, err := lc.Write(write)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to perform write operation")
	}

	return wr, err
}

func (s *publicRpcServer) Read(ctx context.Context, read *proto.ReadRequest) (*proto.ReadResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", read).
		Msg("Write request")

	lc, err := s.shardsDirector.GetLeader(*read.ShardId)
	if err != nil {
		if status.Code(err) != common.CodeNodeIsNotLeader {
			s.log.Warn().Err(err).
				Msg("Failed to get the leader controller")
		}
		return nil, err
	}

	rr, err := lc.Read(read)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to perform read operation")
	}

	return rr, err
}

func (s *publicRpcServer) GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	s.log.Debug().
		Str("peer", common.GetPeer(stream.Context())).
		Interface("req", req).
		Msg("Get notifications")

	lc, err := s.shardsDirector.GetLeader(req.ShardId)
	if err != nil {
		if status.Code(err) != common.CodeNodeIsNotLeader {
			s.log.Warn().Err(err).
				Msg("Failed to get the leader controller")
		}
		return err
	}

	if err = lc.GetNotifications(req, stream); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn().Err(err).
			Msg("Failed to handle notifications request")
	}

	return err
}

func (s *publicRpcServer) Close() error {
	return s.grpcServer.Close()
}
