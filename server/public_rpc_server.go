package server

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
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

//func (s *PublicRpcServer) GetShardsAssignments(_ *proto.Empty, out proto.ClientAPI_GetShardsAssignmentsServer) error {
//	s.shardsDirector.GetShardsAssignments(func(assignments *proto.ShardsAssignments) {
//		out.SendMsg(assignments)
//	})
//	return nil
//}
//
//func (s *PublicRpcServer) Put(ctx context.Context, putOp *proto.PutOp) (*proto.Stat, error) {
//	// TODO make shard ID string in client rpc
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
	return s.container.Close()
}
