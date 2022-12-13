package impl

import (
	"context"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/common"
	"oxia/proto"
	"time"
)

const rpcTimeout = 30 * time.Second

type RpcProvider interface {
	GetShardAssignmentStream(ctx context.Context, node ServerAddress) (proto.OxiaControl_ShardAssignmentClient, error)
	Fence(ctx context.Context, node ServerAddress, req *proto.FenceRequest) (*proto.FenceResponse, error)
	BecomeLeader(ctx context.Context, node ServerAddress, req *proto.BecomeLeaderRequest) (*proto.EmptyResponse, error)
	AddFollower(ctx context.Context, node ServerAddress, req *proto.AddFollowerRequest) (*proto.EmptyResponse, error)

	GetHealthClient(node ServerAddress) (grpc_health_v1.HealthClient, error)
}

type rpcProvider struct {
	pool common.ClientPool
}

func NewRpcProvider(pool common.ClientPool) RpcProvider {
	return &rpcProvider{pool: pool}
}

func (r *rpcProvider) GetShardAssignmentStream(ctx context.Context, node ServerAddress) (proto.OxiaControl_ShardAssignmentClient, error) {
	rpc, err := r.pool.GetControlRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	return rpc.ShardAssignment(ctx)
}

func (r *rpcProvider) Fence(ctx context.Context, node ServerAddress, req *proto.FenceRequest) (*proto.FenceResponse, error) {
	rpc, err := r.pool.GetControlRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.Fence(ctx, req)
}

func (r *rpcProvider) BecomeLeader(ctx context.Context, node ServerAddress, req *proto.BecomeLeaderRequest) (*proto.EmptyResponse, error) {
	rpc, err := r.pool.GetControlRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.BecomeLeader(ctx, req)
}

func (r *rpcProvider) AddFollower(ctx context.Context, node ServerAddress, req *proto.AddFollowerRequest) (*proto.EmptyResponse, error) {
	rpc, err := r.pool.GetControlRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return rpc.AddFollower(ctx, req)
}

func (r *rpcProvider) GetHealthClient(node ServerAddress) (grpc_health_v1.HealthClient, error) {
	return r.pool.GetHealthRpc(node.Internal)
}
