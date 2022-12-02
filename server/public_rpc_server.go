package server

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"oxia/common"
	"oxia/proto"
	"oxia/server/container"
)

type PublicRpcServer struct {
	proto.UnimplementedOxiaClientServer

	shardsDirector       ShardsDirector
	assignmentDispatcher ShardAssignmentsDispatcher
	container            *container.Container
	log                  zerolog.Logger
}

func NewPublicRpcServer(port int, shardsDirector ShardsDirector, assignmentDispatcher ShardAssignmentsDispatcher) (*PublicRpcServer, error) {
	server := &PublicRpcServer{
		shardsDirector:       shardsDirector,
		assignmentDispatcher: assignmentDispatcher,
		log: log.With().
			Str("component", "public-rpc-server").
			Logger(),
	}

	var err error
	server.container, err = container.Start("public", port, func(registrar grpc.ServiceRegistrar) {
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
	err := s.assignmentDispatcher.AddClient(srv)
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

func (s *PublicRpcServer) Close() error {
	return s.container.Close()
}
