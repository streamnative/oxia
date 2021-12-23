package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
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
	proto.UnimplementedInternalAPIServer
	shardsManager ShardsManager

	grpcServer *grpc.Server
}

func newInternalRpcServer(port int, advertisedInternalAddress string, shardsManager ShardsManager) (*internalRpcServer, error) {
	res := &internalRpcServer{
		shardsManager: shardsManager,
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	res.grpcServer = grpc.NewServer()
	proto.RegisterInternalAPIServer(res.grpcServer, res)
	log.Info().
		Str("bindAddress", listener.Addr().String()).
		Str("advertisedAddress", advertisedInternalAddress).
		Msg("Started internal RPC server")

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

func (s *internalRpcServer) UpdateStatus(ctx context.Context, newClusterStatus *proto.ClusterStatus) (*proto.InternalEmpty, error) {
	b, _ := json.Marshal(newClusterStatus)
	log.Info().
		RawJSON("value", b).
		Msg("Received new Cluster status")
	err := s.shardsManager.UpdateClusterStatus(newClusterStatus)
	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to update to new cluster status")
	}
	return &proto.InternalEmpty{}, err
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

func (s *internalRpcServer) Follow(in proto.InternalAPI_FollowServer) error {
	md, ok := metadata.FromIncomingContext(in.Context())
	if !ok {
		return errors.New("There is no metadata header in Follow request")
	}

	shard, err := readHeaderUint32(md, metadataShard)
	if err != nil {
		return err
	}

	slc, err := s.shardsManager.GetLeaderController(shard)
	if err != nil {
		log.Warn().
			Err(err).
			Uint32("shard", shard).
			Msg("This node is not leader for shard")
		return err
	}

	epoch, err := readHeaderUint64(md, metadataEpoch)
	if !ok {
		return err
	}

	firstEntry, err := readHeaderUint64(md, metadataFirstEntry)
	if err != nil {
		return err
	}

	followerName, err := readHeader(md, metadataFollower)
	if err != nil {
		return err
	}

	err = slc.Follow(followerName, firstEntry, epoch, in)
	if err != nil {
		log.Warn().
			Err(err).
			Uint32("shard", shard).
			Uint64("epoch", epoch).
			Uint64("firstEntry", firstEntry).
			Msg("Failed to attach follower")
	}
	return err
}
