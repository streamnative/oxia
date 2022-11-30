package standalone

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"oxia/common"
	"oxia/proto"
	"oxia/server"
	"oxia/server/container"
	"oxia/server/kv"
	"oxia/server/wal"
)

type StandaloneRpcServer struct {
	proto.UnimplementedOxiaClientServer

	advertisedPublicAddress string
	numShards               uint32
	kvFactory               kv.KVFactory
	walFactory              wal.WalFactory
	clientPool              common.ClientPool
	Container               *container.Container
	controllers             map[uint32]server.LeaderController
	assignmentDispatcher    server.ShardAssignmentsDispatcher

	log zerolog.Logger
}

func NewStandaloneRpcServer(port int, advertisedPublicAddress string, numShards uint32, walFactory wal.WalFactory, kvFactory kv.KVFactory) (*StandaloneRpcServer, error) {
	res := &StandaloneRpcServer{
		advertisedPublicAddress: advertisedPublicAddress,
		numShards:               numShards,
		walFactory:              walFactory,
		kvFactory:               kvFactory,
		clientPool:              common.NewClientPool(),
		controllers:             make(map[uint32]server.LeaderController),
		log: log.With().
			Str("component", "standalone-rpc-server").
			Logger(),
	}

	var err error
	for i := uint32(0); i < numShards; i++ {
		var lc server.LeaderController
		if lc, err = server.NewLeaderController(i,
			server.NewReplicationRpcProvider(res.clientPool), res.walFactory, res.kvFactory); err != nil {
			return nil, err
		}

		if _, err := lc.BecomeLeader(&proto.BecomeLeaderRequest{
			ShardId:           i,
			Epoch:             0,
			ReplicationFactor: 1,
			FollowerMaps:      make(map[string]*proto.EntryId),
		}); err != nil {
			return nil, err
		}

		res.controllers[i] = lc
	}

	res.Container, err = container.Start("standalone", port, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaClientServer(registrar, res)
	})
	if err != nil {
		return nil, err
	}

	res.assignmentDispatcher = server.NewStandaloneShardAssignmentDispatcher(
		fmt.Sprintf("%s:%d", advertisedPublicAddress, res.Container.Port()),
		numShards)

	return res, nil
}

func (s *StandaloneRpcServer) Close() error {
	err := s.Container.Close()

	for _, c := range s.controllers {
		err = multierr.Append(err, c.Close())
	}
	return err
}

func (s *StandaloneRpcServer) ShardAssignments(_ *proto.ShardAssignmentsRequest, stream proto.OxiaClient_ShardAssignmentsServer) error {
	return s.assignmentDispatcher.AddClient(stream)
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
