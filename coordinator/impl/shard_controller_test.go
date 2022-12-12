package impl

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"sync"
	"testing"
	"time"
)

func TestShardController(t *testing.T) {
	var shard uint32 = 5
	rpc := newMockRpcProvider()
	coordinator := newMockCoordinator()

	s1 := ServerAddress{"s1:9091", "s1:8191"}
	s2 := ServerAddress{"s2:9091", "s2:8191"}
	s3 := ServerAddress{"s3:9091", "s3:8191"}

	sc := NewShardController(shard, ShardMetadata{
		Status:   ShardStatusUnknown,
		Epoch:    1,
		Leader:   nil,
		Ensemble: []ServerAddress{s1, s2, s3},
	}, rpc, coordinator)

	// Shard controller should initiate a leader election
	// and fence each server
	rpc.GetNode(s1).FenceResponse(2, 1, 0, nil)
	rpc.GetNode(s2).FenceResponse(2, 1, -1, nil)
	rpc.GetNode(s3).FenceResponse(2, 1, -1, nil)

	rpc.GetNode(s1).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 2)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 2)

	// s1 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s1).BecomeLeaderResponse(2, nil)
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Equal(t, ShardStatusSteadyState, sc.Status())
	assert.EqualValues(t, 2, sc.Epoch())
	assert.Equal(t, s1, *sc.Leader())

	// Simulate the failure of the leader
	sc.HandleNodeFailure(s1)

	rpc.FailNode(s1, errors.New("failed to connect"))
	rpc.GetNode(s2).FenceResponse(3, 2, 0, nil)
	rpc.GetNode(s3).FenceResponse(3, 2, -1, nil)

	rpc.GetNode(s1).expectFenceRequest(t, shard, 3)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 3)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 3)

	// s2 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s2).BecomeLeaderResponse(3, nil)
	rpc.GetNode(s2).expectBecomeLeaderRequest(t, shard, 3, 3)

	assert.Eventually(t, func() bool {
		return sc.Status() == ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 3, sc.Epoch())
	assert.Equal(t, s2, *sc.Leader())

	// Simulate the failure of the leader
	sc.HandleNodeFailure(s2)

	rpc.FailNode(s2, errors.New("failed to connect"))
	rpc.GetNode(s3).FenceResponse(4, 2, -1, nil)

	rpc.GetNode(s1).expectFenceRequest(t, shard, 4)
	rpc.GetNode(s2).expectFenceRequest(t, shard, 4)
	rpc.GetNode(s3).expectFenceRequest(t, shard, 4)

	assert.NoError(t, sc.Close())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type sCoordinatorEvents struct {
	shard    uint32
	metadata ShardMetadata
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

func (m *mockCoordinator) ClusterStatus() ClusterStatus {
	panic("not implemented")
}

func (m *mockCoordinator) WaitForNextUpdate(currentValue *proto.ShardAssignmentsResponse) *proto.ShardAssignmentsResponse {
	panic("not implemented")
}

func (m *mockCoordinator) InitiateLeaderElection(shard uint32, metadata ShardMetadata) error {
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

func (m *mockCoordinator) ElectedLeader(shard uint32, metadata ShardMetadata) error {
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

func (m *mockCoordinator) NodeBecameUnavailable(node ServerAddress) {
	panic("not implemented")
}
