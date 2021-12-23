package server

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"net"
	"oxia/proto"
)

type publicRpcServer struct {
	proto.UnimplementedClientAPIServer

	shardsManager ShardsManager

	grpcServer *grpc.Server
	log        zerolog.Logger
}

func newPublicRpcServer(port int, advertisedPublicAddress string, shardsManager ShardsManager) (*publicRpcServer, error) {
	res := &publicRpcServer{
		shardsManager: shardsManager,
		log: log.With().
			Str("component", "public-rpc-server").
			Logger(),
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	res.grpcServer = grpc.NewServer()
	proto.RegisterClientAPIServer(res.grpcServer, res)
	res.log.Info().
		Str("bindAddress", listener.Addr().String()).
		Str("advertisedAddress", advertisedPublicAddress).
		Msg("Started public RPC server")

	go func() {
		if err := res.grpcServer.Serve(listener); err != nil {
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

func (s *publicRpcServer) Close() error {
	s.grpcServer.GracefulStop()
	return nil
}
