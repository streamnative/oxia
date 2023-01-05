package internal

import (
	"context"
	"oxia/common"
	"oxia/proto"
)

type Executor interface {
	ExecuteWrite(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error)
	ExecuteRead(ctx context.Context, request *proto.ReadRequest) (*proto.ReadResponse, error)
}

type ExecutorImpl struct {
	ClientPool     common.ClientPool
	ShardManager   ShardManager
	ServiceAddress string
}

func (e *ExecutorImpl) ExecuteWrite(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	rpc, err := e.rpc(request.ShardId)
	if err != nil {
		return nil, err
	}

	return rpc.Write(ctx, request)
}

func (e *ExecutorImpl) ExecuteRead(ctx context.Context, request *proto.ReadRequest) (*proto.ReadResponse, error) {
	rpc, err := e.rpc(request.ShardId)
	if err != nil {
		return nil, err
	}

	return rpc.Read(ctx, request)
}

func (e *ExecutorImpl) rpc(shardId *uint32) (proto.OxiaClientClient, error) {
	var target string
	if shardId != nil {
		target = e.ShardManager.Leader(*shardId)
	} else {
		target = e.ServiceAddress
	}

	rpc, err := e.ClientPool.GetClientRpc(target)
	if err != nil {
		return nil, err
	}
	return rpc, nil
}
