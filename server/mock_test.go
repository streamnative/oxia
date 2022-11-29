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

func newMockRpcClient() *mockRpcClient {
	return &mockRpcClient{
		addEntryReqs:  make(chan *proto.AddEntryRequest, 1000),
		addEntryResps: make(chan *proto.AddEntryResponse, 1000),
		truncateReqs:  make(chan *proto.TruncateRequest, 1000),
		truncateResps: make(chan struct {
			*proto.TruncateResponse
			error
		}, 1000),
		md: make(metadata.MD),
	}
}

type mockRpcClient struct {
	addEntryReqs  chan *proto.AddEntryRequest
	addEntryResps chan *proto.AddEntryResponse
	truncateReqs  chan *proto.TruncateRequest
	truncateResps chan struct {
		*proto.TruncateResponse
		error
	}
	md metadata.MD
}

func (m *mockRpcClient) Send(request *proto.AddEntryRequest) error {
	m.addEntryReqs <- request
	return nil
}

func (m *mockRpcClient) Recv() (*proto.AddEntryResponse, error) {
	res := <-m.addEntryResps
	return res, nil
}

func (m *mockRpcClient) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *mockRpcClient) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockRpcClient) CloseSend() error {
	close(m.addEntryReqs)
	return nil
}

func (m *mockRpcClient) Context() context.Context {
	panic("not implemented")
}

func (m *mockRpcClient) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockRpcClient) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockRpcClient) GetAddEntriesStream(follower string) (proto.OxiaLogReplication_AddEntriesClient, error) {
	return m, nil
}

func (m *mockRpcClient) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	m.truncateReqs <- req

	// Caller needs to provide response to the channel

	x := <-m.truncateResps
	return x.TruncateResponse, x.error
}
