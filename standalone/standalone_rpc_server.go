package standalone

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"oxia/common/container"
	"oxia/proto"
	"oxia/server"
	"oxia/server/kv"
	"oxia/server/wal"
)

type StandaloneRpcServer struct {
	proto.UnimplementedOxiaClientServer

	advertisedPublicAddress string
	numShards               uint32
	kvFactory               kv.KVFactory
	walFactory              wal.WalFactory
	sessionManager          server.SessionManager
	grpcServer              container.GrpcServer
	controllers             map[uint32]server.LeaderController
	assignmentDispatcher    server.ShardAssignmentsDispatcher
	replicationRpcProvider  server.ReplicationRpcProvider

	log zerolog.Logger
}

func NewStandaloneRpcServer(bindAddress string, advertisedPublicAddress string, numShards uint32, walFactory wal.WalFactory, kvFactory kv.KVFactory) (*StandaloneRpcServer, error) {
	res := &StandaloneRpcServer{
		advertisedPublicAddress: advertisedPublicAddress,
		numShards:               numShards,
		walFactory:              walFactory,
		kvFactory:               kvFactory,
		replicationRpcProvider:  server.NewReplicationRpcProvider(),
		controllers:             make(map[uint32]server.LeaderController),
		log: log.With().
			Str("component", "standalone-rpc-server").
			Logger(),
	}

	res.sessionManager = server.NewSessionManager(func(shardId uint32) (server.LeaderController, error) {
		controller, found := res.controllers[shardId]
		if !found {
			return nil, errors.New("shard not found")
		}
		return controller, nil
	})

	var err error
	for i := uint32(0); i < numShards; i++ {
		var lc server.LeaderController
		if lc, err = server.NewLeaderController(i, res.replicationRpcProvider, res.walFactory, res.kvFactory, res.sessionManager); err != nil {
			return nil, err
		}

		newEpoch := lc.Epoch() + 1

		if _, err := lc.Fence(&proto.FenceRequest{
			ShardId: i,
			Epoch:   newEpoch,
		}); err != nil {
			return nil, err
		}

		if _, err := lc.BecomeLeader(&proto.BecomeLeaderRequest{
			ShardId:           i,
			Epoch:             newEpoch,
			ReplicationFactor: 1,
			FollowerMaps:      make(map[string]*proto.EntryId),
		}); err != nil {
			return nil, err
		}

		res.controllers[i] = lc
	}

	res.grpcServer, err = container.Default.StartGrpcServer("standalone", bindAddress, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaClientServer(registrar, res)
	})
	if err != nil {
		return nil, err
	}

	res.assignmentDispatcher = server.NewStandaloneShardAssignmentDispatcher(
		fmt.Sprintf("%s:%d", advertisedPublicAddress, res.grpcServer.Port()),
		numShards)

	return res, nil
}

func (s *StandaloneRpcServer) Close() error {
	err := multierr.Combine(
		s.assignmentDispatcher.Close(),
		s.grpcServer.Close(),
		s.replicationRpcProvider.Close(),
	)

	for _, c := range s.controllers {
		err = multierr.Append(err, c.Close())
	}
	return multierr.Append(err, s.sessionManager.Close())
}

func (s *StandaloneRpcServer) ShardAssignments(_ *proto.ShardAssignmentsRequest, stream proto.OxiaClient_ShardAssignmentsServer) error {
	return s.assignmentDispatcher.RegisterForUpdates(stream)
}

func (s *StandaloneRpcServer) Port() int {
	return s.grpcServer.Port()
}

func (s *StandaloneRpcServer) Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
	lc, ok := s.controllers[*write.ShardId]
	if !ok {
		return nil, errors.New("shard not found")
	}

	return lc.Write(write)
}

func (s *StandaloneRpcServer) Read(ctx context.Context, read *proto.ReadRequest) (*proto.ReadResponse, error) {
	lc, ok := s.controllers[*read.ShardId]
	if !ok {
		return nil, errors.New("shard not found")
	}

	return lc.Read(read)
}
