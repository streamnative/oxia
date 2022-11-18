package internal

import (
	"context"
	"oxia/common"
	"oxia/proto"
	"time"
)

type Executor interface {
	ExecuteWrite(request *proto.WriteRequest) (*proto.WriteResponse, error)
	ExecuteRead(request *proto.ReadRequest) (*proto.ReadResponse, error)
}

type ExecutorImpl struct {
	ClientPool   common.ClientPool
	ShardManager ShardManager
	ServiceUrl   string
	Timeout      time.Duration
}

func (e *ExecutorImpl) ExecuteWrite(request *proto.WriteRequest) (*proto.WriteResponse, error) {
	rpc, err := e.rpc(request.ShardId)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.Timeout)
	defer cancel()

	return rpc.Write(ctx, request)
}

func (e *ExecutorImpl) ExecuteRead(request *proto.ReadRequest) (*proto.ReadResponse, error) {
	rpc, err := e.rpc(request.ShardId)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.Timeout)
	defer cancel()

	return rpc.Read(ctx, request)
}

func (e *ExecutorImpl) rpc(shardId *uint32) (proto.OxiaClientClient, error) {
	var target string
	if shardId != nil {
		target = e.ShardManager.Leader(*shardId)
	} else {
		target = e.ServiceUrl
	}

	rpc, err := e.ClientPool.GetClientRpc(target)
	if err != nil {
		return nil, err
	}
	return rpc, nil
}
