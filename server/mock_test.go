package server

import (
	"context"
	"google.golang.org/grpc/metadata"
	"oxia/proto"
)

func newMockServerAddEntriesStream() *mockServerAddEntriesStream {
	return &mockServerAddEntriesStream{
		requests:  make(chan *proto.AddEntryRequest, 1000),
		responses: make(chan *proto.AddEntryResponse, 1000),
		md:        make(metadata.MD),
	}
}

type mockServerAddEntriesStream struct {
	requests  chan *proto.AddEntryRequest
	responses chan *proto.AddEntryResponse
	md        metadata.MD
}

func (m *mockServerAddEntriesStream) AddRequest(request *proto.AddEntryRequest) {
	m.requests <- request
}

func (m *mockServerAddEntriesStream) GetResponse() *proto.AddEntryResponse {
	return <-m.responses
}

func (m *mockServerAddEntriesStream) Send(response *proto.AddEntryResponse) error {
	m.responses <- response
	return nil
}

func (m *mockServerAddEntriesStream) Recv() (*proto.AddEntryRequest, error) {
	request := <-m.requests
	return request, nil
}

func (m *mockServerAddEntriesStream) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockServerAddEntriesStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockServerAddEntriesStream) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockServerAddEntriesStream) Context() context.Context {
	return context.Background()
}

func (m *mockServerAddEntriesStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockServerAddEntriesStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

/// Mock of the client side handler

func newMockClientAddEntriesStream() *mockClientAddEntriesStream {
	return &mockClientAddEntriesStream{
		requests:  make(chan *proto.AddEntryRequest, 1000),
		responses: make(chan *proto.AddEntryResponse, 1000),
		md:        make(metadata.MD),
	}
}

type mockClientAddEntriesStream struct {
	requests  chan *proto.AddEntryRequest
	responses chan *proto.AddEntryResponse
	md        metadata.MD
}

func (m *mockClientAddEntriesStream) Send(request *proto.AddEntryRequest) error {
	m.requests <- request
	return nil
}

func (m *mockClientAddEntriesStream) Recv() (*proto.AddEntryResponse, error) {
	res := <-m.responses
	return res, nil
}

func (m *mockClientAddEntriesStream) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *mockClientAddEntriesStream) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockClientAddEntriesStream) CloseSend() error {
	close(m.requests)
	return nil
}

func (m *mockClientAddEntriesStream) Context() context.Context {
	panic("not implemented")
}

func (m *mockClientAddEntriesStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockClientAddEntriesStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockClientAddEntriesStream) GetAddEntriesStream(follower string) (proto.OxiaLogReplication_AddEntriesClient, error) {
	return m, nil
}

func newMockShardAssignmentClientStream() *mockShardAssignmentClientStream {
	return &mockShardAssignmentClientStream{
		responses: make(chan *proto.ShardAssignmentsResponse, 1000),
		md:        make(metadata.MD),
	}
}

type mockShardAssignmentClientStream struct {
	responses chan *proto.ShardAssignmentsResponse
	md        metadata.MD
}

func (m *mockShardAssignmentClientStream) GetResponse() *proto.ShardAssignmentsResponse {
	x := <-m.responses
	return x
}

func (m *mockShardAssignmentClientStream) Send(response *proto.ShardAssignmentsResponse) error {
	m.responses <- response
	return nil
}

func (m *mockShardAssignmentClientStream) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockShardAssignmentClientStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockShardAssignmentClientStream) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockShardAssignmentClientStream) Context() context.Context {
	return context.Background()
}

func (m *mockShardAssignmentClientStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockShardAssignmentClientStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

func newMockShardAssignmentControllerStream() *mockShardAssignmentControllerStream {
	return &mockShardAssignmentControllerStream{
		requests:  make(chan *proto.ShardAssignmentsResponse, 1000),
		responses: make(chan *proto.CoordinationEmpty, 1000),
		md:        make(metadata.MD),
	}
}

type mockShardAssignmentControllerStream struct {
	requests  chan *proto.ShardAssignmentsResponse
	responses chan *proto.CoordinationEmpty
	md        metadata.MD
}

func (m *mockShardAssignmentControllerStream) GetResponse() *proto.CoordinationEmpty {
	return <-m.responses
}

func (m *mockShardAssignmentControllerStream) SendAndClose(empty *proto.CoordinationEmpty) error {
	m.responses <- empty
	return nil
}

func (m *mockShardAssignmentControllerStream) AddRequest(request *proto.ShardAssignmentsResponse) {
	m.requests <- request
}

func (m *mockShardAssignmentControllerStream) Recv() (*proto.ShardAssignmentsResponse, error) {
	request := <-m.requests
	return request, nil
}

func (m *mockShardAssignmentControllerStream) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockShardAssignmentControllerStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockShardAssignmentControllerStream) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockShardAssignmentControllerStream) Context() context.Context {
	return context.Background()
}

func (m *mockShardAssignmentControllerStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockShardAssignmentControllerStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}
