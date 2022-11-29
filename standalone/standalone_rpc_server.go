package standalone

import (
	"context"
	"fmt"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"math"
	"oxia/proto"
	"oxia/server/container"
	"oxia/server/kv"
	"oxia/server/wal"
)

// This implementation is temporary, until all the WAL changes are ready

type StandaloneRpcServer struct {
	proto.UnimplementedOxiaClientServer

	identityAddr string
	numShards    uint32
	dbs          map[uint32]kv.DB
	Container    *container.Container
}

func NewStandaloneRpcServer(port int, identityAddr string, numShards uint32, kvFactory kv.KVFactory) (*StandaloneRpcServer, error) {
	// Assuming 1 single shard
	server := &StandaloneRpcServer{
		identityAddr: identityAddr,
		numShards:    numShards,
		dbs:          make(map[uint32]kv.DB),
	}

	var err error
	for i := uint32(0); i < numShards; i++ {
		if server.dbs[i], err = kv.NewDB(i, kvFactory); err != nil {
			return nil, err
		}
	}

	server.Container, err = container.Start("standalone", port, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaClientServer(registrar, server)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *StandaloneRpcServer) Close() error {
	var errs error
	if err := s.Container.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	for _, db := range s.dbs {
		if err := db.Close(); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

func (s *StandaloneRpcServer) ShardAssignments(_ *proto.ShardAssignmentsRequest, stream proto.OxiaClient_ShardAssignmentsServer) error {
	res := &proto.ShardAssignmentsResponse{
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	}

	bucketSize := math.MaxUint32 / s.numShards

	for i := uint32(0); i < s.numShards; i++ {
		res.Assignments = append(res.Assignments, &proto.ShardAssignment{
			ShardId: i,
			Leader:  fmt.Sprintf("%s:%d", s.identityAddr, s.Container.Port()),
			ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
				Int32HashRange: &proto.Int32HashRange{
					MinHashInclusive: i * bucketSize,
					MaxHashExclusive: (i + 1) * bucketSize,
				},
			},
		})
	}

	return stream.Send(res)
}

func (s *StandaloneRpcServer) Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
	// TODO generate an actual EntryId if needed
	return s.dbs[*write.ShardId].ProcessWrite(write, wal.InvalidOffset)
}

func (s *StandaloneRpcServer) Read(ctx context.Context, read *proto.ReadRequest) (*proto.ReadResponse, error) {
	return s.dbs[*read.ShardId].ProcessRead(read)
}
