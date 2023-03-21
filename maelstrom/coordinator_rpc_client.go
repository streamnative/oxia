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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"os"
	"oxia/common"
	"oxia/coordinator/impl"
	"oxia/coordinator/model"
	"oxia/proto"
	"sync"
)

type maelstromCoordinatorRpcProvider struct {
	sync.Mutex

	dispatcher        *dispatcher
	assignmentStreams map[int64]*maelstromShardAssignmentClient
}

func newRpcProvider(dispatcher *dispatcher) impl.RpcProvider {
	return &maelstromCoordinatorRpcProvider{
		dispatcher:        dispatcher,
		assignmentStreams: map[int64]*maelstromShardAssignmentClient{},
	}
}

func (m *maelstromCoordinatorRpcProvider) PushShardAssignments(ctx context.Context, node model.ServerAddress) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	return newShardAssignmentClient(ctx, m, node.Internal), nil
}

func (m *maelstromCoordinatorRpcProvider) NewTerm(ctx context.Context, node model.ServerAddress, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	if res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeNewTermRequest, req); err != nil {
		return nil, err
	} else {
		return res.(*proto.NewTermResponse), nil
	}
}

func (m *maelstromCoordinatorRpcProvider) BecomeLeader(ctx context.Context, node model.ServerAddress, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	if res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeBecomeLeaderRequest, req); err != nil {
		return nil, err
	} else {
		return res.(*proto.BecomeLeaderResponse), nil
	}
}

func (m *maelstromCoordinatorRpcProvider) AddFollower(ctx context.Context, node model.ServerAddress, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	if res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeAddFollowerRequest, req); err != nil {
		return nil, err
	} else {
		return res.(*proto.AddFollowerResponse), nil
	}
}

func (m *maelstromCoordinatorRpcProvider) GetStatus(ctx context.Context, node model.ServerAddress, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	if res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeGetStatusRequest, req); err != nil {
		return nil, err
	} else {
		return res.(*proto.GetStatusResponse), nil
	}
}

func (m *maelstromCoordinatorRpcProvider) GetHealthClient(node model.ServerAddress) (grpc_health_v1.HealthClient, error) {
	return &maelstromHealthCheckClient{
		provider: m,
		node:     node.Internal,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type maelstromShardAssignmentClient struct {
	BaseStream

	provider *maelstromCoordinatorRpcProvider
	node     string
	streamId int64

	ctx    context.Context
	cancel context.CancelFunc
}

func newShardAssignmentClient(ctx context.Context, provider *maelstromCoordinatorRpcProvider, node string) proto.OxiaCoordination_PushShardAssignmentsClient {
	sac := &maelstromShardAssignmentClient{
		provider: provider,
		node:     node,
		streamId: msgIdGenerator.Add(1),
	}

	sac.ctx, sac.cancel = context.WithCancel(ctx)

	provider.Lock()
	defer provider.Unlock()
	provider.assignmentStreams[sac.streamId] = sac

	return sac
}

func (m *maelstromShardAssignmentClient) Send(response *proto.ShardAssignments) error {
	m.provider.dispatcher.currentLeader = response.Namespaces[common.DefaultNamespace].Assignments[0].Leader
	req := &Message[OxiaStreamMessage]{
		Src:  thisNode,
		Dest: m.node,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  MsgTypeShardAssignmentsResponse,
				MsgId: msgIdGenerator.Add(1),
			},
			OxiaMsg:  toJson(response),
			StreamId: m.streamId,
		},
	}

	b, _ := json.Marshal(req)
	fmt.Fprintln(os.Stdout, string(b))
	return nil
}

func (m *maelstromShardAssignmentClient) Context() context.Context {
	return m.ctx
}

func (m *maelstromShardAssignmentClient) CloseAndRecv() (*proto.CoordinationShardAssignmentsResponse, error) {
	return &proto.CoordinationShardAssignmentsResponse{}, nil
}

type BaseStream struct {
}

func (m *BaseStream) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *BaseStream) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *BaseStream) CloseSend() error {
	return nil
}

func (m *BaseStream) Context() context.Context {
	return context.Background()
}

func (m *BaseStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *BaseStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *maelstromReplicateServerStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *maelstromReplicateServerStream) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

/// Client health check

type maelstromHealthCheckClient struct {
	node     string
	provider *maelstromCoordinatorRpcProvider
}

type maelstromHealthCheckClientStream struct {
	BaseStream
	sentFirst bool
	ctx       context.Context
}

func (m *maelstromHealthCheckClient) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	if _, err := m.provider.dispatcher.RpcRequest(ctx, m.node, MsgTypeHealthCheck, &proto.GetStatusRequest{}); err != nil {
		// If we're failing the health-check, also close the assignments stream
		m.provider.Lock()
		defer m.provider.Unlock()
		for sid, s := range m.provider.assignmentStreams {
			if s.node == m.node {
				s.cancel()
				delete(m.provider.assignmentStreams, sid)
			}
		}
		return nil, err
	}

	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (m *maelstromHealthCheckClient) Watch(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	if _, err := m.Check(ctx, in, opts...); err != nil {
		return nil, err
	}

	return &maelstromHealthCheckClientStream{
		ctx: ctx,
	}, nil
}

func (m *maelstromHealthCheckClientStream) Recv() (*grpc_health_v1.HealthCheckResponse, error) {
	if !m.sentFirst {
		m.sentFirst = true
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
	}

	<-m.ctx.Done()
	return nil, m.ctx.Err()
}
