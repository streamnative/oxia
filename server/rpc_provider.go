// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	ctx = metadata.AppendToOutgoingContext(ctx, common.MetadataShardId, fmt.Sprintf("%d", shard))

	stream, err := rpc.AddEntries(ctx)
	return stream, err
}

func (r *replicationRpcProvider) SendSnapshot(ctx context.Context, follower string, shard uint32) (
	proto.OxiaLogReplication_SendSnapshotClient, error) {
	rpc, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		return nil, err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, common.MetadataShardId, fmt.Sprintf("%d", shard))

	stream, err := rpc.SendSnapshot(ctx)
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
