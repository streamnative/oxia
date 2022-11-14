package server

import (
	"context"
	"google.golang.org/grpc/metadata"
	"oxia/proto"
)

func newMockAddEntriesStream() *mockAddEntriesStream {
	return &mockAddEntriesStream{
		requests:  make(chan *proto.AddEntryRequest, 1000),
		responses: make(chan *proto.AddEntryResponse, 1000),
		md:        make(metadata.MD),
	}
}

type mockAddEntriesStream struct {
	requests  chan *proto.AddEntryRequest
	responses chan *proto.AddEntryResponse
	md        metadata.MD
}

func (m *mockAddEntriesStream) AddRequest(request *proto.AddEntryRequest) {
	m.requests <- request
}

func (m *mockAddEntriesStream) GetResponse() *proto.AddEntryResponse {
	return <-m.responses
}

func (m *mockAddEntriesStream) Send(response *proto.AddEntryResponse) error {
	m.responses <- response
	return nil
}

func (m *mockAddEntriesStream) Recv() (*proto.AddEntryRequest, error) {
	request := <-m.requests
	return request, nil
}

func (m *mockAddEntriesStream) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockAddEntriesStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockAddEntriesStream) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockAddEntriesStream) Context() context.Context {
	return context.Background()
}

func (m *mockAddEntriesStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockAddEntriesStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}
