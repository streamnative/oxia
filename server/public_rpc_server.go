package server

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"oxia/common"
	"oxia/common/container"
	"oxia/proto"
)

type PublicRpcServer struct {
	proto.UnimplementedOxiaClientServer

	shardsDirector       ShardsDirector
	assignmentDispatcher ShardAssignmentsDispatcher
	sessionManager       SessionManager
	grpcServer           container.GrpcServer
	log                  zerolog.Logger
}

func NewPublicRpcServer(provider container.GrpcProvider, bindAddress string, shardsDirector ShardsDirector, assignmentDispatcher ShardAssignmentsDispatcher, sessionManager SessionManager) (*PublicRpcServer, error) {
	server := &PublicRpcServer{
		shardsDirector:       shardsDirector,
		assignmentDispatcher: assignmentDispatcher,
		sessionManager:       sessionManager,
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

func (s *PublicRpcServer) ShardAssignments(_ *proto.ShardAssignmentsRequest, srv proto.OxiaClient_ShardAssignmentsServer) error {
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

func (s *PublicRpcServer) Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", write).
		Msg("Write request")

	lc, err := s.shardsDirector.GetLeader(*write.ShardId)
	if err != nil {
		if !errors.Is(err, ErrorNodeIsNotLeader) {
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

func (s *PublicRpcServer) Read(ctx context.Context, read *proto.ReadRequest) (*proto.ReadResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", read).
		Msg("Write request")

	lc, err := s.shardsDirector.GetLeader(*read.ShardId)
	if err != nil {
		if !errors.Is(err, ErrorNodeIsNotLeader) {
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

func (s *PublicRpcServer) GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	s.log.Debug().
		Str("peer", common.GetPeer(stream.Context())).
		Interface("req", req).
		Msg("Get notifications")

	lc, err := s.shardsDirector.GetLeader(*req.ShardId)
	if err != nil {
		if !errors.Is(err, ErrorNodeIsNotLeader) {
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

func (s *PublicRpcServer) CreateSession(ctx context.Context, req *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", req).
		Msg("Create session request")
	res, err := s.sessionManager.CreateSession(req)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to create session")
		return nil, err
	}
	return res, nil
}

func (s *PublicRpcServer) KeepAlive(stream proto.OxiaClient_KeepAliveServer) error {
	s.log.Debug().
		Str("peer", common.GetPeer(stream.Context())).
		Msg("Session keep alive")
	err := s.sessionManager.KeepAlive(stream)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to keep alive session")
		return err
	}
	return nil
}

func (s *PublicRpcServer) CloseSession(ctx context.Context, req *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	s.log.Debug().
		Str("peer", common.GetPeer(ctx)).
		Interface("req", req).
		Msg("Close session request")
	res, err := s.sessionManager.CloseSession(req)
	if err != nil {
		s.log.Warn().Err(err).
			Msg("Failed to close session")
		return nil, err
	}
	return res, nil
}

func (s *PublicRpcServer) Close() error {
	return s.grpcServer.Close()
}
