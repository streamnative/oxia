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
	"oxia/coordination"
)

const (
	metadataFollower   = "follower"
	metadataShard      = "shard"
	metadataEpoch      = "epoch"
	metadataFirstEntry = "first_entry"
)

type coordinationRpcServer struct {
	coordination.UnimplementedOxiaCoordinationServer
	shardsDirector ShardsDirector

	grpcServer *grpc.Server
	log        zerolog.Logger
}

func newCoordinationRpcServer(port int, advertisedInternalAddress string, shardsDirector ShardsDirector) (*coordinationRpcServer, error) {
	res := &coordinationRpcServer{
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
	coordination.RegisterOxiaCoordinationServer(res.grpcServer, res)
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

func (s *coordinationRpcServer) Close() error {
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

func callShardManager[T any](c context.Context, s *coordinationRpcServer, f func(ShardManager) (T, error)) (T, error) {
	var zeroT T
	md, ok := metadata.FromIncomingContext(c)
	if !ok {
		return zeroT, errors.New("There is no metadata header in request")
	}
	shard, err := readHeaderUint32(md, metadataShard)
	if err != nil {
		return zeroT, err
	}
	manager, err := s.shardsDirector.GetManager(shard, true)
	if err != nil {
		return zeroT, err
	}
	response, err := f(manager)
	return response, err
}

func (s *coordinationRpcServer) Fence(c context.Context, req *coordination.FenceRequest) (*coordination.FenceResponse, error) {
	response, err := callShardManager[*coordination.FenceResponse](c, s, func(m ShardManager) (*coordination.FenceResponse, error) {
		response, err := m.Fence(req)
		return response, err
	})
	return response, err
}
func (s *coordinationRpcServer) BecomeLeader(c context.Context, req *coordination.BecomeLeaderRequest) (*coordination.BecomeLeaderResponse, error) {
	response, err := callShardManager[*coordination.BecomeLeaderResponse](c, s, func(m ShardManager) (*coordination.BecomeLeaderResponse, error) {
		response, err := m.BecomeLeader(req)
		return response, err
	})
	return response, err
}
func (s *coordinationRpcServer) AddFollower(c context.Context, req *coordination.AddFollowerRequest) (*coordination.CoordinationEmpty, error) {

	response, err := callShardManager[*coordination.CoordinationEmpty](c, s, func(m ShardManager) (*coordination.CoordinationEmpty, error) {
		response, err := m.AddFollower(req)
		return response, err
	})
	return response, err
}
func (s *coordinationRpcServer) Truncate(c context.Context, req *coordination.TruncateRequest) (*coordination.TruncateResponse, error) {
	response, err := callShardManager[*coordination.TruncateResponse](c, s, func(m ShardManager) (*coordination.TruncateResponse, error) {
		response, err := m.Truncate(req)
		return response, err
	})
	return response, err
}
func (s *coordinationRpcServer) AddEntries(srv coordination.OxiaCoordination_AddEntriesServer) error {
	_, err := callShardManager[any](srv.Context(), s, func(m ShardManager) (any, error) {
		response, err := m.AddEntries(srv)
		return response, err
	})
	return err
}
func (s *coordinationRpcServer) PrepareReconfig(c context.Context, req *coordination.PrepareReconfigRequest) (*coordination.PrepareReconfigResponse, error) {
	response, err := callShardManager[*coordination.PrepareReconfigResponse](c, s, func(m ShardManager) (*coordination.PrepareReconfigResponse, error) {
		response, err := m.PrepareReconfig(req)
		return response, err
	})
	return response, err
}
func (s *coordinationRpcServer) Snapshot(c context.Context, req *coordination.SnapshotRequest) (*coordination.SnapshotResponse, error) {
	response, err := callShardManager[*coordination.SnapshotResponse](c, s, func(m ShardManager) (*coordination.SnapshotResponse, error) {
		response, err := m.Snapshot(req)
		return response, err
	})
	return response, err
}
func (s *coordinationRpcServer) CommitReconfig(c context.Context, req *coordination.CommitReconfigRequest) (*coordination.CommitReconfigResponse, error) {
	response, err := callShardManager[*coordination.CommitReconfigResponse](c, s, func(m ShardManager) (*coordination.CommitReconfigResponse, error) {
		response, err := m.CommitReconfig(req)
		return response, err
	})
	return response, err
}
