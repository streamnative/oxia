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

package impl

import (
	"context"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/proto"
)

const rpcTimeout = 30 * time.Second

type RpcProvider interface {
	PushShardAssignments(ctx context.Context, node model.ServerAddress, req *proto.ShardAssignments) (*proto.CoordinationShardAssignmentsResponse, error)
	NewTerm(ctx context.Context, node model.ServerAddress, req *proto.NewTermRequest) (*proto.NewTermResponse, error)
	BecomeLeader(ctx context.Context, node model.ServerAddress, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)
	AddFollower(ctx context.Context, node model.ServerAddress, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)
	GetStatus(ctx context.Context, node model.ServerAddress, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	DeleteShard(ctx context.Context, node model.ServerAddress, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)

	GetHealthClient(node model.ServerAddress) (grpc_health_v1.HealthClient, error)
}

type rpcProvider struct {
	pool common.ClientPool
}

func NewRpcProvider(pool common.ClientPool) RpcProvider {
	return &rpcProvider{pool: pool}
}

func (r *rpcProvider) PushShardAssignments(ctx context.Context, node model.ServerAddress, req *proto.ShardAssignments) (*proto.CoordinationShardAssignmentsResponse, error) {
	rpc, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	return rpc.PushShardAssignments(ctx, req)
}

func (r *rpcProvider) NewTerm(ctx context.Context, node model.ServerAddress, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	rpc, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.NewTerm(ctx, req)
}

func (r *rpcProvider) BecomeLeader(ctx context.Context, node model.ServerAddress, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	rpc, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.BecomeLeader(ctx, req)
}

func (r *rpcProvider) AddFollower(ctx context.Context, node model.ServerAddress, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	rpc, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.AddFollower(ctx, req)
}

func (r *rpcProvider) GetStatus(ctx context.Context, node model.ServerAddress, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	rpc, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.GetStatus(ctx, req)
}

func (r *rpcProvider) DeleteShard(ctx context.Context, node model.ServerAddress, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	rpc, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.DeleteShard(ctx, req)
}

func (r *rpcProvider) GetHealthClient(node model.ServerAddress) (grpc_health_v1.HealthClient, error) {
	return r.pool.GetHealthRpc(node.Internal)
}
