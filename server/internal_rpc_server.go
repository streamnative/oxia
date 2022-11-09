package server

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net"
	"oxia/proto"
)

const (
	metadataFollower   = "follower"
	metadataShard      = "shard"
	metadataEpoch      = "epoch"
	metadataFirstEntry = "first_entry"
)

type internalRpcServer struct {
	proto.UnimplementedOxiaControlServer
	proto.UnimplementedOxiaLogReplicationServer
	shardsDirector ShardsDirector

	grpcServer *grpc.Server
	log        zerolog.Logger
}

func newCoordinationRpcServer(port int, advertisedInternalAddress string, shardsDirector ShardsDirector) (*internalRpcServer, error) {
	res := &internalRpcServer{
		shardsDirector: shardsDirector,
		log: log.With().
			Str("component", "coordination-rpc-server").
			Logger(),
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	res.grpcServer = grpc.NewServer()
	proto.RegisterOxiaControlServer(res.grpcServer, res)
	proto.RegisterOxiaLogReplicationServer(res.grpcServer, res)
	res.log.Info().
		Str("bindAddress", listener.Addr().String()).
		Str("advertisedAddress", advertisedInternalAddress).
		Msg("Started coordination RPC server")

	go func() {
		if err := res.grpcServer.Serve(listener); err != nil {
			log.Fatal().Err(err).Msg("Failed to serve")
		}
	}()

	return res, nil
}

func (s *internalRpcServer) Close() error {
	s.grpcServer.GracefulStop()
	return nil
}

func readHeader(md metadata.MD, key string) (value string, err error) {
	arr := md.Get(key)
	if len(arr) == 0 {
		return "", errors.Errorf("Request must include '%s' metadata field", key)
	}

	if len(arr) > 1 {
		return "", errors.Errorf("Request must include '%s' metadata field only once", key)
	}
	return arr[0], nil
}

func readHeaderUint64(md metadata.MD, key string) (v uint64, err error) {
	s, err := readHeader(md, key)
	if err != nil {
		return 0, err
	}

	var r uint64
	_, err = fmt.Sscan(s, &r)
	return r, err
}

func readHeaderUint32(md metadata.MD, key string) (v uint32, err error) {
	s, err := readHeader(md, key)
	if err != nil {
		return 0, err
	}

	var r uint32
	_, err = fmt.Sscan(s, &r)
	return r, err
}

func callShardManager[T any](c context.Context, s *internalRpcServer, f func(ShardManager, string) (T, error)) (T, error) {
	var zeroT T
	md, ok := metadata.FromIncomingContext(c)
	if !ok {
		return zeroT, errors.New("There is no metadata header in request")
	}
	shard, err := readHeader(md, metadataShard)
	if err != nil {
		return zeroT, err
	}
	source, err := readHeader(md, "source_node")
	if err != nil {
		return zeroT, err
	}

	manager, err := s.shardsDirector.GetManager(ShardId(shard), true)
	if err != nil {
		return zeroT, err
	}
	response, err := f(manager, source)
	return response, err
}

func (s *internalRpcServer) Fence(c context.Context, req *proto.FenceRequest) (*proto.FenceResponse, error) {
	response, err := callShardManager[*proto.FenceResponse](c, s, func(m ShardManager, source string) (*proto.FenceResponse, error) {
		response, err := m.Fence(req)
		return response, err
	})
	return response, err
}
func (s *internalRpcServer) BecomeLeader(c context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	response, err := callShardManager[*proto.BecomeLeaderResponse](c, s, func(m ShardManager, source string) (*proto.BecomeLeaderResponse, error) {
		response, err := m.BecomeLeader(req)
		return response, err
	})
	return response, err
}
func (s *internalRpcServer) AddFollower(c context.Context, req *proto.AddFollowerRequest) (*proto.CoordinationEmpty, error) {

	response, err := callShardManager[*proto.CoordinationEmpty](c, s, func(m ShardManager, source string) (*proto.CoordinationEmpty, error) {
		response, err := m.AddFollower(req)
		return response, err
	})
	return response, err
}
func (s *internalRpcServer) Truncate(c context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	response, err := callShardManager[*proto.TruncateResponse](c, s, func(m ShardManager, source string) (*proto.TruncateResponse, error) {
		response, err := m.Truncate(source, req)
		return response, err
	})
	return response, err
}
func (s *internalRpcServer) AddEntries(srv proto.OxiaLogReplication_AddEntriesServer) error {
	_, err := callShardManager[any](srv.Context(), s, func(m ShardManager, source string) (any, error) {
		response, err := m.AddEntries(source, srv)
		return response, err
	})
	return err
}
func (s *internalRpcServer) PrepareReconfig(c context.Context, req *proto.PrepareReconfigRequest) (*proto.PrepareReconfigResponse, error) {
	response, err := callShardManager[*proto.PrepareReconfigResponse](c, s, func(m ShardManager, source string) (*proto.PrepareReconfigResponse, error) {
		response, err := m.PrepareReconfig(req)
		return response, err
	})
	return response, err
}
func (s *internalRpcServer) Snapshot(c context.Context, req *proto.SnapshotRequest) (*proto.SnapshotResponse, error) {
	response, err := callShardManager[*proto.SnapshotResponse](c, s, func(m ShardManager, source string) (*proto.SnapshotResponse, error) {
		response, err := m.Snapshot(req)
		return response, err
	})
	return response, err
}
func (s *internalRpcServer) CommitReconfig(c context.Context, req *proto.CommitReconfigRequest) (*proto.CommitReconfigResponse, error) {
	response, err := callShardManager[*proto.CommitReconfigResponse](c, s, func(m ShardManager, source string) (*proto.CommitReconfigResponse, error) {
		response, err := m.CommitReconfig(req)
		return response, err
	})
	return response, err
}
