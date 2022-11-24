package server

import (
	"context"
	"google.golang.org/grpc/metadata"
	"oxia/proto"
)

func newMockServerAddEntriesStream() *mockServerAddEntriesStream {
	return &mockServerAddEntriesStream{
		mockServerStream: mockServerStream[*proto.AddEntryRequest, *proto.AddEntryResponse]{
			requests:  make(chan *proto.AddEntryRequest, 1000),
			responses: make(chan *proto.AddEntryResponse, 1000),
			md:        make(metadata.MD),
		},
	}
}

type mockServerStream[Req any, Res any] struct {
	requests  chan Req
	responses chan Res
	md        metadata.MD
}

type mockServerAddEntriesStream struct {
	mockServerStream[*proto.AddEntryRequest, *proto.AddEntryResponse]
}

func (m *mockServerStream[Req, Res]) AddRequest(request Req) {
	m.requests <- request
}

func (m *mockServerStream[Req, Res]) GetResponse() Res {
	return <-m.responses
}

func (m *mockServerStream[Req, Res]) Send(response Res) error {
	m.responses <- response
	return nil
}

func (m *mockServerStream[Req, Res]) Recv() (Req, error) {
	request := <-m.requests
	return request, nil
}

func (m *mockServerStream[Req, Res]) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockServerStream[Req, Res]) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockServerStream[Req, Res]) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockServerStream[Req, Res]) Context() context.Context {
	return context.Background()
}

func (m *mockServerStream[Req, Res]) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockServerStream[Req, Res]) RecvMsg(msg interface{}) error {
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
