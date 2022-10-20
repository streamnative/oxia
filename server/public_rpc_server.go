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

type PublicRpcServer struct {
	proto.UnimplementedClientAPIServer

	shardsDirector ShardsDirector

	grpcServer *grpc.Server
	log        zerolog.Logger
}

func NewPublicRpcServer(port int, advertisedPublicAddress string, shardsDirector ShardsDirector) (*PublicRpcServer, error) {
	res := &PublicRpcServer{
		shardsDirector: shardsDirector,
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

func (s *PublicRpcServer) GetShardsAssignments(_ *proto.Empty, out proto.ClientAPI_GetShardsAssignmentsServer) error {
	s.shardsDirector.GetShardsAssignments(func(assignments *proto.ShardsAssignments) {
		out.SendMsg(assignments)
	})
	return nil
}

func (s *PublicRpcServer) Put(ctx context.Context, putOp *proto.PutOp) (*proto.Stat, error) {
	slc, err := s.shardsDirector.GetManager(putOp.GetShardId(), false)
	if err != nil {
		return nil, err
	}

	return slc.Put(putOp)
}

func (s *PublicRpcServer) Close() error {
	s.grpcServer.GracefulStop()
	return nil
}
