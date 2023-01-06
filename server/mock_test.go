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
		sendSnapshotStream: newMockSendSnapshotClientStream(context.Background()),
		addEntryReqs:       make(chan *proto.AddEntryRequest, 1000),
		addEntryResps:      make(chan *proto.AddEntryResponse, 1000),
		truncateReqs:       make(chan *proto.TruncateRequest, 1000),
		truncateResps: make(chan struct {
			*proto.TruncateResponse
			error
		}, 1000),
		md: make(metadata.MD),
	}
}

type mockRpcClient struct {
	sendSnapshotStream *mockSendSnapshotClientStream
	addEntryReqs       chan *proto.AddEntryRequest
	addEntryResps      chan *proto.AddEntryResponse
	truncateReqs       chan *proto.TruncateRequest
	truncateResps      chan struct {
		*proto.TruncateResponse
		error
	}
	md metadata.MD
}

func (m *mockRpcClient) Close() error {
	return nil
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

func (m *mockRpcClient) GetAddEntriesStream(ctx context.Context, follower string, shard uint32) (proto.OxiaLogReplication_AddEntriesClient, error) {
	return m, nil
}

func (m *mockRpcClient) SendSnapshot(ctx context.Context, follower string, shard uint32) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	return m.sendSnapshotStream, nil
}

func (m *mockRpcClient) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	m.truncateReqs <- req

	// Caller needs to provide response to the channel

	x := <-m.truncateResps
	return x.TruncateResponse, x.error
}

func newMockShardAssignmentClientStream() *mockShardAssignmentClientStream {
	r := &mockShardAssignmentClientStream{
		responses: make(chan *proto.ShardAssignmentsResponse, 1000),
		md:        make(metadata.MD),
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r
}

type mockShardAssignmentClientStream struct {
	responses chan *proto.ShardAssignmentsResponse
	md        metadata.MD
	ctx       context.Context
	cancel    context.CancelFunc
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
	return m.ctx
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
		responses: make(chan *proto.CoordinationShardAssignmentsResponse, 1000),
		md:        make(metadata.MD),
	}
}

type mockShardAssignmentControllerStream struct {
	requests  chan *proto.ShardAssignmentsResponse
	responses chan *proto.CoordinationShardAssignmentsResponse
	md        metadata.MD
}

func (m *mockShardAssignmentControllerStream) GetResponse() *proto.CoordinationShardAssignmentsResponse {
	return <-m.responses
}

func (m *mockShardAssignmentControllerStream) SendAndClose(empty *proto.CoordinationShardAssignmentsResponse) error {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newMockServerSendSnapshotStream() *mockServerSendSnapshotStream {
	return &mockServerSendSnapshotStream{
		chunks:    make(chan *proto.SnapshotChunk, 1000),
		responses: make(chan *proto.SnapshotResponse, 1000),
		md:        make(metadata.MD),
	}
}

type mockServerSendSnapshotStream struct {
	chunks    chan *proto.SnapshotChunk
	responses chan *proto.SnapshotResponse
	md        metadata.MD
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

func (m *mockServerSendSnapshotStream) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockServerSendSnapshotStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockServerSendSnapshotStream) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockServerSendSnapshotStream) Context() context.Context {
	return context.Background()
}

func (m *mockServerSendSnapshotStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockServerSendSnapshotStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type mockGetNotificationsServer struct {
	ch  chan *proto.NotificationBatch
	ctx context.Context
}

func newMockGetNotificationsServer(ctx context.Context) *mockGetNotificationsServer {
	return &mockGetNotificationsServer{
		ch:  make(chan *proto.NotificationBatch, 100),
		ctx: ctx,
	}
}

func (m *mockGetNotificationsServer) Send(batch *proto.NotificationBatch) error {
	m.ch <- batch
	return nil
}

func (m *mockGetNotificationsServer) SetHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockGetNotificationsServer) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockGetNotificationsServer) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockGetNotificationsServer) Context() context.Context {
	return m.ctx
}

func (m *mockGetNotificationsServer) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockGetNotificationsServer) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

//////

func newMockSendSnapshotClientStream(ctx context.Context) *mockSendSnapshotClientStream {
	r := &mockSendSnapshotClientStream{
		requests: make(chan *proto.SnapshotChunk, 100),
		response: make(chan *proto.SnapshotResponse, 1),
		md:       make(metadata.MD),
	}

	r.ctx, r.cancel = context.WithCancel(ctx)
	return r
}

type mockSendSnapshotClientStream struct {
	requests chan *proto.SnapshotChunk
	response chan *proto.SnapshotResponse
	md       metadata.MD
	ctx      context.Context
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

func (m *mockSendSnapshotClientStream) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *mockSendSnapshotClientStream) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockSendSnapshotClientStream) CloseSend() error {
	m.cancel()
	return nil
}

func (m *mockSendSnapshotClientStream) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockSendSnapshotClientStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (m *mockSendSnapshotClientStream) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockSendSnapshotClientStream) Context() context.Context {
	return m.ctx
}

func (m *mockSendSnapshotClientStream) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockSendSnapshotClientStream) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

//////

func newMockKeepAliveServer() *mockKeepAliveServer {
	r := &mockKeepAliveServer{
		requests: make(chan *proto.SessionHeartbeat, 100),
		response: make(chan *proto.KeepAliveResponse, 1),
		md:       make(metadata.MD),
	}

	return r
}

type mockKeepAliveServer struct {
	requests chan *proto.SessionHeartbeat
	response chan *proto.KeepAliveResponse
	md       metadata.MD
}

func (m *mockKeepAliveServer) SendAndClose(empty *proto.KeepAliveResponse) error {
	m.response <- empty
	close(m.response)
	return nil
}

func (m *mockKeepAliveServer) Recv() (*proto.SessionHeartbeat, error) {
	return <-m.requests, nil
}

func (m *mockKeepAliveServer) sendHeartbeat(heartbeat *proto.SessionHeartbeat) {
	m.requests <- heartbeat
}

func (m *mockKeepAliveServer) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockKeepAliveServer) SendHeader(md metadata.MD) error {
	panic("implement me")
}

func (m *mockKeepAliveServer) SetTrailer(md metadata.MD) {
	panic("implement me")
}

func (m *mockKeepAliveServer) Context() context.Context {
	return context.Background()
}

func (_ *mockKeepAliveServer) SendMsg(m interface{}) error {
	panic("implement me")
}

func (_ *mockKeepAliveServer) RecvMsg(m interface{}) error {
	panic("implement me")
}
