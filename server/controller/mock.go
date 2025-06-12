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

package controller

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/streamnative/oxia/proto"
)

func NewMockServerReplicateStream() *MockServerReplicateStream {
	return &MockServerReplicateStream{
		requests:  make(chan *proto.Append, 1000),
		responses: make(chan *proto.Ack, 1000),
	}
}

type MockServerReplicateStream struct {
	mockBase
	requests  chan *proto.Append
	responses chan *proto.Ack
}

func (m *MockServerReplicateStream) AddRequest(request *proto.Append) {
	m.requests <- request
}

func (m *MockServerReplicateStream) GetResponse() *proto.Ack {
	return <-m.responses
}

func (m *MockServerReplicateStream) Send(response *proto.Ack) error {
	m.responses <- response
	return nil
}

func (m *MockServerReplicateStream) Recv() (*proto.Append, error) {
	request := <-m.requests
	return request, nil
}

// Mock of the client side handler

func newMockRpcClient() *mockRpcClient {
	return &mockRpcClient{
		sendSnapshotStream: newMockSendSnapshotClientStream(context.Background()),
		appendReqs:         make(chan *proto.Append, 1000),
		ackResps:           make(chan *proto.Ack, 1000),
		truncateReqs:       make(chan *proto.TruncateRequest, 1000),
		truncateResps: make(chan struct {
			*proto.TruncateResponse
			error
		}, 1000),
	}
}

type mockRpcClient struct {
	mockBase
	sendSnapshotStream *mockSendSnapshotClientStream
	appendReqs         chan *proto.Append
	ackResps           chan *proto.Ack
	truncateReqs       chan *proto.TruncateRequest
	truncateResps      chan struct {
		*proto.TruncateResponse
		error
	}
}

func (*mockRpcClient) Close() error {
	return nil
}

func (m *mockRpcClient) Send(request *proto.Append) error {
	m.appendReqs <- request
	return nil
}

func (m *mockRpcClient) Recv() (*proto.Ack, error) {
	res := <-m.ackResps
	return res, nil
}

func (*mockRpcClient) CloseSend() error {
	return nil
}

func (m *mockRpcClient) GetReplicateStream(context.Context, string, string, int64, int64) (proto.OxiaLogReplication_ReplicateClient, error) {
	return m, nil
}

func (m *mockRpcClient) SendSnapshot(context.Context, string, string, int64, int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	return m.sendSnapshotStream, nil
}

func (m *mockRpcClient) Truncate(_ string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	m.truncateReqs <- req

	// Caller needs to provide response to the channel

	x := <-m.truncateResps
	return x.TruncateResponse, x.error
}

func NewMockShardAssignmentClientStream() *MockShardAssignmentClientStream {
	r := &MockShardAssignmentClientStream{
		responses: make(chan *proto.ShardAssignments, 1000),
	}

	r.ctx, r.Cancel = context.WithCancel(context.Background())
	return r
}

type MockShardAssignmentClientStream struct {
	mockBase
	responses chan *proto.ShardAssignments
	Cancel    context.CancelFunc
}

func (m *MockShardAssignmentClientStream) GetResponse() *proto.ShardAssignments {
	x := <-m.responses
	return x
}

func (m *MockShardAssignmentClientStream) Send(response *proto.ShardAssignments) error {
	m.responses <- response
	return nil
}

func NewMockShardAssignmentControllerStream() *MockShardAssignmentControllerStream {
	return &MockShardAssignmentControllerStream{
		requests:  make(chan *proto.ShardAssignments, 1000),
		responses: make(chan *proto.CoordinationShardAssignmentsResponse, 1000),
	}
}

type MockShardAssignmentControllerStream struct {
	mockBase
	requests  chan *proto.ShardAssignments
	responses chan *proto.CoordinationShardAssignmentsResponse
}

func (m *MockShardAssignmentControllerStream) GetResponse() *proto.CoordinationShardAssignmentsResponse {
	return <-m.responses
}

func (m *MockShardAssignmentControllerStream) SendAndClose(empty *proto.CoordinationShardAssignmentsResponse) error {
	m.responses <- empty
	return nil
}

func (m *MockShardAssignmentControllerStream) AddRequest(request *proto.ShardAssignments) {
	m.requests <- request
}

func (m *MockShardAssignmentControllerStream) Recv() (*proto.ShardAssignments, error) {
	request := <-m.requests
	return request, nil
}

func newMockServerSendSnapshotStream() *mockServerSendSnapshotStream {
	return &mockServerSendSnapshotStream{
		chunks:    make(chan *proto.SnapshotChunk, 1000),
		responses: make(chan *proto.SnapshotResponse, 1000),
	}
}

type mockServerSendSnapshotStream struct {
	mockBase
	chunks    chan *proto.SnapshotChunk
	responses chan *proto.SnapshotResponse
}

func (m *mockServerSendSnapshotStream) AddChunk(chunk *proto.SnapshotChunk) {
	m.chunks <- chunk
}

func (m *mockServerSendSnapshotStream) GetResponse() *proto.SnapshotResponse {
	return <-m.responses
}

func (m *mockServerSendSnapshotStream) SendAndClose(empty *proto.SnapshotResponse) error {
	m.responses <- empty
	return nil
}

func (m *mockServerSendSnapshotStream) Recv() (*proto.SnapshotChunk, error) {
	return <-m.chunks, nil
}

type mockGetNotificationsServer struct {
	mockBase
	ch chan *proto.NotificationBatch
}

func newMockGetNotificationsServer(ctx context.Context) *mockGetNotificationsServer {
	r := &mockGetNotificationsServer{
		ch: make(chan *proto.NotificationBatch, 100),
	}
	r.ctx = ctx
	return r
}

func (m *mockGetNotificationsServer) Send(batch *proto.NotificationBatch) error {
	m.ch <- batch
	return nil
}

func newMockSendSnapshotClientStream(ctx context.Context) *mockSendSnapshotClientStream {
	r := &mockSendSnapshotClientStream{
		requests: make(chan *proto.SnapshotChunk, 100),
		response: make(chan *proto.SnapshotResponse, 1),
	}

	r.ctx, r.cancel = context.WithCancel(ctx)
	return r
}

type mockSendSnapshotClientStream struct {
	mockBase
	requests chan *proto.SnapshotChunk
	response chan *proto.SnapshotResponse
	cancel   context.CancelFunc
}

func (m *mockSendSnapshotClientStream) Send(chunk *proto.SnapshotChunk) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.requests <- chunk:
		return nil
	}
}

func (m *mockSendSnapshotClientStream) CloseAndRecv() (*proto.SnapshotResponse, error) {
	m.cancel()
	close(m.requests)
	return <-m.response, nil
}

func (m *mockSendSnapshotClientStream) CloseSend() error {
	m.cancel()
	return nil
}

type mockBase struct {
	md  metadata.MD
	ctx context.Context
}

func (*mockBase) SendHeader(_ metadata.MD) error {
	panic("not implemented")
}

func (*mockBase) RecvMsg(any) error {
	panic("not implemented")
}

func (*mockBase) SendMsg(any) error {
	panic("not implemented")
}

func (*mockBase) SetTrailer(metadata.MD) {
	panic("not implemented")
}

func (*mockBase) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (*mockBase) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockBase) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockBase) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
