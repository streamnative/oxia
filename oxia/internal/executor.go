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
	"fmt"
	"sync"

	"google.golang.org/grpc/metadata"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
)

type Executor interface {
	ExecuteWrite(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error)
	ExecuteRead(ctx context.Context, request *proto.ReadRequest) (proto.OxiaClient_ReadClient, error)
	ExecuteList(ctx context.Context, request *proto.ListRequest) (proto.OxiaClient_ListClient, error)
	ExecuteRangeScan(ctx context.Context, request *proto.RangeScanRequest) (proto.OxiaClient_RangeScanClient, error)
}

type executorImpl struct {
	sync.RWMutex

	ClientPool     common.ClientPool
	ShardManager   ShardManager
	ServiceAddress string

	writeStreams map[int64]*streamWrapper

	ctx       context.Context
	namespace string
}

func NewExecutor(ctx context.Context, namespace string, pool common.ClientPool, manager ShardManager, serviceAddress string) Executor {
	e := &executorImpl{
		ctx:            ctx,
		namespace:      namespace,
		ClientPool:     pool,
		ShardManager:   manager,
		ServiceAddress: serviceAddress,
		writeStreams:   make(map[int64]*streamWrapper),
	}

	return e
}

func (e *executorImpl) ExecuteWrite(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	sw, err := e.writeStream(request.Shard) //nolint:contextcheck
	if err != nil {
		return nil, err
	}

	return sw.Send(ctx, request)
}

func (e *executorImpl) ExecuteRead(ctx context.Context, request *proto.ReadRequest) (proto.OxiaClient_ReadClient, error) {
	rpc, err := e.rpc(request.Shard)
	if err != nil {
		return nil, err
	}

	return rpc.Read(ctx, request)
}

func (e *executorImpl) ExecuteList(ctx context.Context, request *proto.ListRequest) (proto.OxiaClient_ListClient, error) {
	rpc, err := e.rpc(request.Shard)
	if err != nil {
		return nil, err
	}

	return rpc.List(ctx, request)
}

func (e *executorImpl) ExecuteRangeScan(ctx context.Context, request *proto.RangeScanRequest) (proto.OxiaClient_RangeScanClient, error) {
	rpc, err := e.rpc(request.Shard)
	if err != nil {
		return nil, err
	}

	return rpc.RangeScan(ctx, request)
}

func (e *executorImpl) rpc(shardId *int64) (proto.OxiaClientClient, error) {
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

func (e *executorImpl) writeStream(shardId *int64) (*streamWrapper, error) {
	e.RLock()

	sw, ok := e.writeStreams[*shardId]
	if ok {
		e.RUnlock()
		return sw, nil
	}

	e.RUnlock()

	rpc, err := e.rpc(shardId)
	if err != nil {
		return nil, err
	}

	ctx := metadata.AppendToOutgoingContext(e.ctx, common.MetadataNamespace, e.namespace)
	ctx = metadata.AppendToOutgoingContext(ctx, common.MetadataShardId, fmt.Sprintf("%d", *shardId))

	stream, err := rpc.WriteStream(ctx)
	if err != nil {
		return nil, err
	}

	sw = newStreamWrapper(stream)

	e.Lock()
	defer e.Unlock()

	e.writeStreams[*shardId] = sw
	return sw, nil
}
