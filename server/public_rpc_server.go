package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"net"
	"oxia/proto"
)

type publicRpcServer struct {
	proto.UnimplementedClientAPIServer

	shardsManager ShardsManager
}

func newPublicRpcServer(port int, shardsManager ShardsManager) (*publicRpcServer, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	res := &publicRpcServer{
		shardsManager: shardsManager,
	}
	s := grpc.NewServer()
	proto.RegisterClientAPIServer(s, res)
	log.Info().
		Str("address", lis.Addr().String()).
		Msg("Started public RPC server")

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("Failed to serve")
		}
	}()

	return res, nil
}

func (s *publicRpcServer) GetShardsAssignments(_ *proto.Empty, out proto.ClientAPI_GetShardsAssignmentsServer) error {
	s.shardsManager.GetShardsAssignments(func(assignments *proto.ShardsAssignments) {
		out.SendMsg(assignments)
	})
	return nil
}

func (s *publicRpcServer) Put(context.Context, *proto.PutOp) (*proto.Stat, error) {
	panic("not implemented")
}
