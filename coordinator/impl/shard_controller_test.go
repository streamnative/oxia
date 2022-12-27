package impl

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"oxia/coordinator/model"
	"oxia/proto"
	"sync"
	"testing"
	"time"
)

func TestShardController(t *testing.T) {
	var shard uint32 = 5
	rpc := newMockRpcProvider()
	coordinator := newMockCoordinator()

	s1 := model.ServerAddress{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.ServerAddress{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.ServerAddress{Public: "s3:9091", Internal: "s3:8191"}

	sc := NewShardController(shard, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Epoch:    1,
		Leader:   nil,
		Ensemble: []model.ServerAddress{s1, s2, s3},
	}, rpc, coordinator)

	// Shard controller should initiate a leader election
	// and fence each server
	rpc.GetNode(s1).FenceResponse(1, 0, nil)
	rpc.GetNode(s2).FenceResponse(1, -1, nil)
	rpc.GetNode(s3).FenceResponse(1, -1, nil)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 2)

	// s1 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Eventually(t, func() bool {
		return sc.Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Epoch())
	assert.Equal(t, s1, *sc.Leader())

	rpc.GetNode(s2).FenceResponse(2, 0, nil)
	rpc.GetNode(s3).FenceResponse(2, -1, nil)

	rpc.GetNode(s2).BecomeLeaderResponse(nil)

	// Simulate the failure of the leader
	rpc.FailNode(s1, errors.New("failed to connect"))
	sc.HandleNodeFailure(s1)

	rpc.GetNode(s1).expectFenceRequest(t, shard, 3)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 3)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 3)

	// s2 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s2).expectBecomeLeaderRequest(t, shard, 3, 3)

	assert.Eventually(t, func() bool {
		return sc.Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 3, sc.Epoch())
	assert.Equal(t, s2, *sc.Leader())

	// Simulate the failure of the leader
	sc.HandleNodeFailure(s2)

	rpc.FailNode(s2, errors.New("failed to connect"))
	rpc.GetNode(s3).FenceResponse(2, -1, nil)

	rpc.GetNode(s1).expectFenceRequest(t, shard, 3)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 4)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 4)

	rpc.GetNode(s1).expectFenceRequest(t, shard, 4)
	assert.NoError(t, sc.Close())
}

func TestShardController_StartingWithLeaderAlreadyPresent(t *testing.T) {
	var shard uint32 = 5
	rpc := newMockRpcProvider()
	coordinator := newMockCoordinator()

	s1 := model.ServerAddress{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.ServerAddress{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.ServerAddress{Public: "s3:9091", Internal: "s3:8191"}

	sc := NewShardController(shard, model.ShardMetadata{
		Status:   model.ShardStatusSteadyState,
		Epoch:    1,
		Leader:   &s1,
		Ensemble: []model.ServerAddress{s1, s2, s3},
	}, rpc, coordinator)

	select {
	case <-rpc.GetNode(s1).fenceRequests:
		assert.Fail(t, "shouldn't have received any fence requests")
	case <-rpc.GetNode(s2).fenceRequests:
		assert.Fail(t, "shouldn't have received any fence requests")
	case <-rpc.GetNode(s3).fenceRequests:
		assert.Fail(t, "shouldn't have received any fence requests")

	case <-time.After(1 * time.Second):
		// Ok
	}

	assert.NoError(t, sc.Close())
}

func TestShardController_FenceWithNonRespondingServer(t *testing.T) {
	var shard uint32 = 5
	rpc := newMockRpcProvider()
	coordinator := newMockCoordinator()

	s1 := model.ServerAddress{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.ServerAddress{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.ServerAddress{Public: "s3:9091", Internal: "s3:8191"}

	sc := NewShardController(shard, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Epoch:    1,
		Leader:   nil,
		Ensemble: []model.ServerAddress{s1, s2, s3},
	}, rpc, coordinator)

	timeStart := time.Now()

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	// Shard controller should initiate a leader election
	// and fence each server
	rpc.GetNode(s1).FenceResponse(1, 0, nil)
	rpc.GetNode(s2).FenceResponse(1, -1, nil)
	// s3 is not responding

	rpc.GetNode(s1).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 2)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.WithinDuration(t, timeStart, time.Now(), 1*time.Second)
	assert.Equal(t, model.ShardStatusSteadyState, sc.Status())
	assert.EqualValues(t, 2, sc.Epoch())
	assert.Equal(t, s1, *sc.Leader())

	assert.NoError(t, sc.Close())
}

func TestShardController_FenceFollowerUntilItRecovers(t *testing.T) {
	var shard uint32 = 5
	rpc := newMockRpcProvider()
	coordinator := newMockCoordinator()

	s1 := model.ServerAddress{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.ServerAddress{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.ServerAddress{Public: "s3:9091", Internal: "s3:8191"}

	sc := NewShardController(shard, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Epoch:    1,
		Leader:   nil,
		Ensemble: []model.ServerAddress{s1, s2, s3},
	}, rpc, coordinator)

	// s3 is failing, though we can still elect a leader
	rpc.GetNode(s1).FenceResponse(1, 0, nil)
	rpc.GetNode(s2).FenceResponse(1, -1, nil)
	rpc.GetNode(s3).FenceResponse(1, -1, errors.New("fails"))

	rpc.GetNode(s1).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 2)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Eventually(t, func() bool {
		return sc.Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Epoch())
	assert.NotNil(t, sc.Leader())
	assert.Equal(t, s1, *sc.Leader())

	// One more failure from s1
	rpc.GetNode(s3).FenceResponse(1, -1, errors.New("fails"))
	rpc.GetNode(s3).expectFenceRequest(t, shard, 2)

	// Now it succeeds
	rpc.GetNode(s3).FenceResponse(1, -1, nil)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 2)

	// Leader should be notified
	rpc.GetNode(s1).AddFollowerResponse(nil)
	rpc.GetNode(s1).expectAddFollowerRequest(t, shard, 2)

	assert.NoError(t, sc.Close())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type sCoordinatorEvents struct {
	shard    uint32
	metadata model.ShardMetadata
}

type mockCoordinator struct {
	sync.Mutex
	err                      error
	initiatedLeaderElections chan sCoordinatorEvents
	electedLeaders           chan sCoordinatorEvents
}

func newMockCoordinator() Coordinator {
	return &mockCoordinator{
		initiatedLeaderElections: make(chan sCoordinatorEvents, 100),
		electedLeaders:           make(chan sCoordinatorEvents, 100),
	}
}

func (m *mockCoordinator) Close() error {
	return nil
}

func (m *mockCoordinator) ClusterStatus() model.ClusterStatus {
	panic("not implemented")
}

func (m *mockCoordinator) WaitForNextUpdate(currentValue *proto.ShardAssignmentsResponse) *proto.ShardAssignmentsResponse {
	panic("not implemented")
}

func (m *mockCoordinator) InitiateLeaderElection(shard uint32, metadata model.ShardMetadata) error {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		err := m.err
		m.err = nil
		return err
	}

	m.initiatedLeaderElections <- sCoordinatorEvents{
		shard:    shard,
		metadata: metadata,
	}
	return nil
}

func (m *mockCoordinator) ElectedLeader(shard uint32, metadata model.ShardMetadata) error {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		err := m.err
		m.err = nil
		return err
	}

	m.electedLeaders <- sCoordinatorEvents{shard, metadata}
	return nil
}

func (m *mockCoordinator) NodeBecameUnavailable(node model.ServerAddress) {
	panic("not implemented")
}
