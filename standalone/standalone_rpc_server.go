package standalone

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"net"
	"oxia/proto"
	"oxia/server"
	"oxia/server/kv"
	"oxia/server/wal"
)

// This implementation is temporary, until all the WAL changes are ready

type StandaloneRpcServer struct {
	proto.UnimplementedOxiaClientServer

	identityAddr          string
	port                  int
	dbs                   map[uint32]kv.DB
	grpcServer            *grpc.Server
	log                   zerolog.Logger
	assignmentsDispatcher server.ShardAssignmentsDispatcher
}

func NewStandaloneRpcServer(port int, identityAddr string, numShards uint32, kvFactory kv.KVFactory) (*StandaloneRpcServer, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	// Assuming 1 single shard
	res := &StandaloneRpcServer{
		identityAddr: identityAddr,
		dbs:          make(map[uint32]kv.DB),
		log: log.With().
			Str("component", "standalone-rpc-server").
			Logger(),
		port:                  listener.Addr().(*net.TCPAddr).Port,
		assignmentsDispatcher: server.NewStandaloneShardAssignmentDispatcher(fmt.Sprintf("%s:%d", identityAddr, port), numShards),
	}

	for i := uint32(0); i < numShards; i++ {
		if res.dbs[i], err = kv.NewDB(i, kvFactory); err != nil {
			return nil, err
		}
	}

	res.grpcServer = grpc.NewServer()
	proto.RegisterOxiaClientServer(res.grpcServer, res)
	res.log.Info().
		Str("bindAddress", listener.Addr().String()).
		Str("identityAddr", identityAddr).
		Msg("Started Standalone RPC server")

	go func() {
		if err := res.grpcServer.Serve(listener); err != nil {
			log.Fatal().Err(err).Msg("Failed to serve")
		}
	}()

	return res, nil
}

func (s *StandaloneRpcServer) Close() error {
	s.grpcServer.GracefulStop()
	for _, db := range s.dbs {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *StandaloneRpcServer) ShardAssignments(_ *proto.ShardAssignmentsRequest, stream proto.OxiaClient_ShardAssignmentsServer) error {
	return s.assignmentsDispatcher.AddClient(stream)
}

func (s *StandaloneRpcServer) Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
	// TODO generate an actual EntryId if needed
	return s.dbs[*write.ShardId].ProcessWrite(write, wal.NonExistentEntryId)
}

func (s *StandaloneRpcServer) Read(ctx context.Context, read *proto.ReadRequest) (*proto.ReadResponse, error) {
	return s.dbs[*read.ShardId].ProcessRead(read)
}

func (s *StandaloneRpcServer) Port() int {
	return s.port
}
