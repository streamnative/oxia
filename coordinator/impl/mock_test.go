package impl

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"
	"oxia/common"
	"oxia/proto"
	"sync"
	"testing"
	"time"
)

func init() {
	common.ConfigureLogger()
}

type mockShardAssignmentsProvider struct {
	sync.Mutex
	cond    *sync.Cond
	current *proto.ShardAssignmentsResponse
}

func newMockShardAssignmentsProvider() *mockShardAssignmentsProvider {
	sap := &mockShardAssignmentsProvider{
		current: nil,
	}

	sap.cond = sync.NewCond(sap)
	return sap
}

func (sap *mockShardAssignmentsProvider) set(value *proto.ShardAssignmentsResponse) {
	sap.Lock()
	defer sap.Unlock()

	sap.current = value
	sap.cond.Broadcast()
}

func (sap *mockShardAssignmentsProvider) WaitForNextUpdate(currentValue *proto.ShardAssignmentsResponse) *proto.ShardAssignmentsResponse {
	sap.Lock()
	defer sap.Unlock()

	for pb.Equal(currentValue, sap.current) {
		sap.cond.Wait()
	}

	return sap.current
}

/////////////////////////////////////////////////////////////////

type mockNodeAvailabilityListener struct {
	events chan ServerAddress
}

func newMockNodeAvailabilityListener() *mockNodeAvailabilityListener {
	return &mockNodeAvailabilityListener{
		events: make(chan ServerAddress, 100),
	}
}

func (nal *mockNodeAvailabilityListener) NodeBecameUnavailable(node ServerAddress) {
	nal.events <- node
}

/////////////////////////////////////////////////////////////////

type mockPerNodeChannels struct {
	fenceRequests  chan *proto.FenceRequest
	fenceResponses chan struct {
		*proto.FenceResponse
		error
	}

	becomeLeaderRequests  chan *proto.BecomeLeaderRequest
	becomeLeaderResponses chan struct {
		*proto.BecomeLeaderResponse
		error
	}

	addFollowerRequests  chan *proto.AddFollowerRequest
	addFollowerResponses chan struct {
		*proto.AddFollowerResponse
		error
	}

	shardAssignmentsStream *mockShardAssignmentClient
	healthClient           *mockHealthClient
	err                    error
}

func (m *mockPerNodeChannels) expectBecomeLeaderRequest(t *testing.T, shard uint32, epoch int64, replicationFactor uint32) {
	r := <-m.becomeLeaderRequests

	assert.Equal(t, shard, r.ShardId)
	assert.Equal(t, epoch, r.Epoch)
	assert.Equal(t, replicationFactor, r.ReplicationFactor)
}

func (m *mockPerNodeChannels) expectFenceRequest(t *testing.T, shard uint32, epoch int64) {
	r := <-m.fenceRequests

	assert.Equal(t, shard, r.ShardId)
	assert.Equal(t, epoch, r.Epoch)
}

func (m *mockPerNodeChannels) expectAddFollowerRequest(t *testing.T, shard uint32, epoch int64) {
	r := <-m.addFollowerRequests

	assert.Equal(t, shard, r.ShardId)
	assert.Equal(t, epoch, r.Epoch)
}

func (m *mockPerNodeChannels) FenceResponse(reqEpoch int64, epoch int64, offset int64, err error) {
	m.fenceResponses <- struct {
		*proto.FenceResponse
		error
	}{&proto.FenceResponse{
		Epoch: reqEpoch,
		HeadIndex: &proto.EntryId{
			Epoch:  epoch,
			Offset: offset,
		},
	}, err}
}

func (m *mockPerNodeChannels) BecomeLeaderResponse(epoch int64, err error) {
	m.becomeLeaderResponses <- struct {
		*proto.BecomeLeaderResponse
		error
	}{&proto.BecomeLeaderResponse{
		Epoch: epoch,
	}, err}
}

func (m *mockPerNodeChannels) AddFollowerResponse(epoch int64, err error) {
	m.addFollowerResponses <- struct {
		*proto.AddFollowerResponse
		error
	}{&proto.AddFollowerResponse{
		Epoch: epoch,
	}, err}
}

func newMockPerNodeChannels() *mockPerNodeChannels {
	return &mockPerNodeChannels{
		fenceRequests: make(chan *proto.FenceRequest, 100),
		fenceResponses: make(chan struct {
			*proto.FenceResponse
			error
		}, 100),
		becomeLeaderRequests: make(chan *proto.BecomeLeaderRequest, 100),
		becomeLeaderResponses: make(chan struct {
			*proto.BecomeLeaderResponse
			error
		}, 100),
		addFollowerRequests: make(chan *proto.AddFollowerRequest, 100),
		addFollowerResponses: make(chan struct {
			*proto.AddFollowerResponse
			error
		}, 100),
		shardAssignmentsStream: newMockShardAssignmentClient(),
		healthClient:           newMockHealthClient(),
	}
}

type mockRpcProvider struct {
	sync.Mutex
	channels map[string]*mockPerNodeChannels
}

func newMockRpcProvider() *mockRpcProvider {
	return &mockRpcProvider{
		channels: make(map[string]*mockPerNodeChannels),
	}
}

func (r *mockRpcProvider) FailNode(node ServerAddress, err error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.err = err
}

func (r *mockRpcProvider) RecoverNode(node ServerAddress) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.err = nil
}

func (r *mockRpcProvider) GetNode(node ServerAddress) *mockPerNodeChannels {
	r.Lock()
	defer r.Unlock()

	return r.getNode(node)
}

func (r *mockRpcProvider) getNode(node ServerAddress) *mockPerNodeChannels {
	res, ok := r.channels[node.Internal]
	if ok {
		return res
	}

	res = newMockPerNodeChannels()
	r.channels[node.Internal] = res
	return res
}

func (r *mockRpcProvider) GetShardAssignmentStream(ctx context.Context, node ServerAddress) (proto.OxiaControl_ShardAssignmentClient, error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	if n.err != nil {
		return nil, n.err
	}
	return n.shardAssignmentsStream, nil
}

func (r *mockRpcProvider) Fence(ctx context.Context, node ServerAddress, req *proto.FenceRequest) (*proto.FenceResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.fenceRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.fenceResponses:
		return response.FenceResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) BecomeLeader(ctx context.Context, node ServerAddress, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.becomeLeaderRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.becomeLeaderResponses:
		return response.BecomeLeaderResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) AddFollower(ctx context.Context, node ServerAddress, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.addFollowerRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.addFollowerResponses:
		return response.AddFollowerResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) GetHealthClient(node ServerAddress) (grpc_health_v1.HealthClient, error) {
	return r.GetNode(node).healthClient, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type mockShardAssignmentClient struct {
	sync.Mutex

	err     error
	updates chan *proto.ShardAssignmentsResponse
}

func newMockShardAssignmentClient() *mockShardAssignmentClient {
	return &mockShardAssignmentClient{
		updates: make(chan *proto.ShardAssignmentsResponse, 100),
	}
}

func (m *mockShardAssignmentClient) SetError(err error) {
	m.Lock()
	defer m.Unlock()

	m.err = err
}

func (m *mockShardAssignmentClient) Send(response *proto.ShardAssignmentsResponse) error {
	m.Lock()
	defer m.Unlock()

	if m.err != nil {
		err := m.err
		m.err = nil
		return err
	}

	m.updates <- response
	return nil
}

func (m *mockShardAssignmentClient) CloseAndRecv() (*proto.CoordinationShardAssignmentsResponse, error) {
	panic("not implemented")
}

func (m *mockShardAssignmentClient) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *mockShardAssignmentClient) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockShardAssignmentClient) CloseSend() error {
	panic("not implemented")
}

func (m *mockShardAssignmentClient) Context() context.Context {
	panic("not implemented")
}

func (m *mockShardAssignmentClient) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockShardAssignmentClient) RecvMsg(msg interface{}) error {
	panic("not implemented")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type mockHealthClient struct {
	sync.Mutex

	status  grpc_health_v1.HealthCheckResponse_ServingStatus
	err     error
	watches []*mockHealthWatchClient
}

func newMockHealthClient() *mockHealthClient {
	return &mockHealthClient{
		status:  grpc_health_v1.HealthCheckResponse_SERVING,
		watches: make([]*mockHealthWatchClient, 0),
	}
}

func (m *mockHealthClient) SetStatus(status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	m.Lock()
	defer m.Unlock()

	m.status = status
	m.err = nil
	for _, w := range m.watches {
		w.responses <- struct {
			*grpc_health_v1.HealthCheckResponse
			error
		}{&grpc_health_v1.HealthCheckResponse{
			Status: status,
		}, nil}
	}
}

func (m *mockHealthClient) SetError(err error) {
	m.Lock()
	defer m.Unlock()

	m.err = err
	for _, w := range m.watches {
		w.responses <- struct {
			*grpc_health_v1.HealthCheckResponse
			error
		}{&grpc_health_v1.HealthCheckResponse{
			Status: m.status,
		}, m.err}
	}
}

func (m *mockHealthClient) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &grpc_health_v1.HealthCheckResponse{Status: m.status}, nil
}

func (m *mockHealthClient) Watch(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	m.Lock()
	defer m.Unlock()

	w := newMockHealthWatchClient(ctx)
	m.watches = append(m.watches, w)
	return w, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type mockHealthWatchClient struct {
	ctx       context.Context
	responses chan struct {
		*grpc_health_v1.HealthCheckResponse
		error
	}
}

func newMockHealthWatchClient(ctx context.Context) *mockHealthWatchClient {
	return &mockHealthWatchClient{
		ctx: ctx,
		responses: make(chan struct {
			*grpc_health_v1.HealthCheckResponse
			error
		}, 100),
	}
}

func (m *mockHealthWatchClient) Recv() (*grpc_health_v1.HealthCheckResponse, error) {
	select {
	case r := <-m.responses:
		return r.HealthCheckResponse, r.error
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockHealthWatchClient) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *mockHealthWatchClient) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockHealthWatchClient) CloseSend() error {
	panic("not implemented")
}

func (m *mockHealthWatchClient) Context() context.Context {
	panic("not implemented")
}

func (m *mockHealthWatchClient) SendMsg(msg interface{}) error {
	panic("not implemented")
}

func (m *mockHealthWatchClient) RecvMsg(msg interface{}) error {
	panic("not implemented")
}