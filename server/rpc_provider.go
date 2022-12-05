package server

import (
	"context"
	"fmt"
	"google.golang.org/grpc/metadata"
	"oxia/common"
	"oxia/proto"
	"time"
)

const rpcTimeout = 30 * time.Second

type ReplicationRpcProvider interface {
	AddEntriesStreamProvider

	Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error)
}

type replicationRpcProvider struct {
	pool common.ClientPool
}

func NewReplicationRpcProvider(pool common.ClientPool) ReplicationRpcProvider {
	return &replicationRpcProvider{pool: pool}
}

func (r *replicationRpcProvider) GetAddEntriesStream(follower string, shard uint32) (
	proto.OxiaLogReplication_AddEntriesClient, error) {
	rpc, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		return nil, err
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), metadataShardId, fmt.Sprintf("%d", shard))

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
