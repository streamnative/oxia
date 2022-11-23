package server

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"net"
	"oxia/proto"
)

type PublicRpcServer struct {
	proto.UnimplementedOxiaClientServer

	shardsDirector       ShardsDirector
	assignmentDispatcher ShardAssignmentsDispatcher

	grpcServer *grpc.Server
	log        zerolog.Logger
}

func NewPublicRpcServer(
	port int,
	advertisedPublicAddress string,
	shardsDirector ShardsDirector,
	assignmentDispatcher ShardAssignmentsDispatcher) (*PublicRpcServer, error) {
	res := &PublicRpcServer{
		shardsDirector:       shardsDirector,
		assignmentDispatcher: assignmentDispatcher,
		log: log.With().
			Str("component", "public-rpc-server").
			Logger(),
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	res.grpcServer = grpc.NewServer()
	proto.RegisterOxiaClientServer(res.grpcServer, res)
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

//func (s *PublicRpcServer) GetShardsAssignments(_ *proto.Empty, out proto.ClientAPI_GetShardsAssignmentsServer) error {
//	s.shardsDirector.GetShardsAssignments(func(assignments *proto.ShardsAssignments) {
//		out.SendMsg(assignments)
//	})
//	return nil
//}
//
//func (s *PublicRpcServer) Put(ctx context.Context, putOp *proto.PutOp) (*proto.Stat, error) {
//	// TODO make shardAssignment ID string in client rpc
//	slc, err := s.shardsDirector.GetManager(ShardId(strconv.FormatInt(int64(putOp.GetShardId()), 10)), false)
//	if err != nil {
//		return nil, err
//	}
//
//	return slc.Write(putOp)
//}
//
//func (s *PublicRpcServer) Get(ctx context.Context, getOp *proto.GetOp) (*proto.GetResult, error) {
//	_, err := s.shardsDirector.GetManager(ShardId(strconv.FormatInt(int64(getOp.GetShardId()), 10)), false)
//	if err != nil {
//		return nil, err
//	}
//	// TODO Read from KVStore directly? Only if leader
//	return nil, nil
//}

func (s *PublicRpcServer) Close() error {
	s.grpcServer.GracefulStop()
	return nil
}
