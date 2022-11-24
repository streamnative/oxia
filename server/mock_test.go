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
