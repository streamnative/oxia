package server

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"oxia/proto"
	"oxia/server/container"
)

const (
	metadataShardId = "shard-id"
)

type internalRpcServer struct {
	proto.UnimplementedOxiaControlServer
	proto.UnimplementedOxiaLogReplicationServer

	shardsDirector ShardsDirector
	container      *container.Container
	log            zerolog.Logger
}

func newCoordinationRpcServer(port int, shardsDirector ShardsDirector) (*internalRpcServer, error) {
	server := &internalRpcServer{
		shardsDirector: shardsDirector,
		log: log.With().
			Str("component", "coordination-rpc-server").
			Logger(),
	}

	var err error
	server.container, err = container.Start("internal", port, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaControlServer(registrar, server)
		proto.RegisterOxiaLogReplicationServer(registrar, server)
		grpc_health_v1.RegisterHealthServer(registrar, health.NewServer())
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *internalRpcServer) Close() error {
	return s.container.Close()
}

func (s *internalRpcServer) Fence(c context.Context, req *proto.FenceRequest) (*proto.FenceResponse, error) {
	// Fence applies to both followers and leaders
	// First check if we have already a follower controller running
	if follower, err := s.shardsDirector.GetFollower(req.ShardId); err != nil {
		if !errors.Is(err, ErrorNodeIsNotFollower) {
			return nil, err
		}

		// If we don't have a follower, fallback to checking the leader controller
	} else {
		return follower.Fence(req)
	}

	if leader, err := s.shardsDirector.GetOrCreateLeader(req.ShardId); err != nil {
		return nil, err
	} else {
		return leader.Fence(req)
	}
}

func (s *internalRpcServer) BecomeLeader(c context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	if leader, err := s.shardsDirector.GetOrCreateLeader(req.ShardId); err != nil {
		return nil, err
	} else {
		return leader.BecomeLeader(req)
	}
}

func (s *internalRpcServer) Truncate(c context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	if follower, err := s.shardsDirector.GetOrCreateFollower(req.ShardId); err != nil {
		return nil, err
	} else {
		return follower.Truncate(req)
	}
}

func (s *internalRpcServer) AddEntries(srv proto.OxiaLogReplication_AddEntriesServer) error {
	// Add entries receives an incoming stream of request, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return errors.New("shard id is not set in the request metadata")
	}

	shardId, err := readHeaderUint32(md, metadataShardId)
	if err != nil {
		return err
	}

	if follower, err := s.shardsDirector.GetOrCreateFollower(shardId); err != nil {
		return err
	} else {
		return follower.AddEntries(srv)
	}
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

func readHeaderUint32(md metadata.MD, key string) (v uint32, err error) {
	s, err := readHeader(md, key)
	if err != nil {
		return 0, err
	}

	var r uint32
	_, err = fmt.Sscan(s, &r)
	return r, err
}
