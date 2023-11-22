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

package internal

import (
	"context"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
)

type Executor interface {
	ExecuteWrite(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error)
	ExecuteRead(ctx context.Context, request *proto.ReadRequest) (proto.OxiaClient_ReadClient, error)
	ExecuteList(ctx context.Context, request *proto.ListRequest) (proto.OxiaClient_ListClient, error)
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

func (e *ExecutorImpl) ExecuteRead(ctx context.Context, request *proto.ReadRequest) (proto.OxiaClient_ReadClient, error) {
	rpc, err := e.rpc(request.ShardId)
	if err != nil {
		return nil, err
	}

	return rpc.Read(ctx, request)
}

func (e *ExecutorImpl) ExecuteList(ctx context.Context, request *proto.ListRequest) (proto.OxiaClient_ListClient, error) {
	rpc, err := e.rpc(request.ShardId)
	if err != nil {
		return nil, err
	}

	return rpc.List(ctx, request)
}

func (e *ExecutorImpl) rpc(shardId *int64) (proto.OxiaClientClient, error) {
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
