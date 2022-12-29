package server

import (
	"context"
	"fmt"
	"google.golang.org/grpc/metadata"
	"io"
	"oxia/common"
	"oxia/proto"
	"time"
)

const rpcTimeout = 30 * time.Second

type ReplicationRpcProvider interface {
	io.Closer
	AddEntriesStreamProvider

	Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error)
}

type replicationRpcProvider struct {
	pool common.ClientPool
}

func NewReplicationRpcProvider() ReplicationRpcProvider {
	return &replicationRpcProvider{
		pool: common.NewClientPool(),
	}
}

func (r *replicationRpcProvider) GetAddEntriesStream(ctx context.Context, follower string, shard uint32) (
	proto.OxiaLogReplication_AddEntriesClient, error) {
	rpc, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		return nil, err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, metadataShardId, fmt.Sprintf("%d", shard))

	stream, err := rpc.AddEntries(ctx)
	return stream, err
}

func (r *replicationRpcProvider) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	rpc, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	return rpc.Truncate(ctx, req)
}

func (r *replicationRpcProvider) Close() error {
	return r.pool.Close()
}
