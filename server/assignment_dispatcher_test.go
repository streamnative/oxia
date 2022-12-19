package server

import (
	"github.com/stretchr/testify/assert"
	"math"
	"oxia/proto"
	"sync"
	"testing"
	"time"
)

func TestUninitializedAssignmentDispatcher(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher()
	mockClient := newMockShardAssignmentClientStream()
	assert.False(t, dispatcher.Initialized())
	err := dispatcher.RegisterForUpdates(mockClient)
	assert.ErrorIs(t, err, ErrorNotInitialized)
	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_Initialized(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher()
	coordinatorStream := newMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.ShardAssignment(coordinatorStream)
		assert.NoError(t, err)
	}()

	assert.False(t, dispatcher.Initialized())
	coordinatorStream.AddRequest(&proto.ShardAssignmentsResponse{
		Assignments: []*proto.ShardAssignment{
			newShardAssignment(0, "server1", 0, 100),
			newShardAssignment(1, "server2", 100, math.MaxUint32),
		},
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	})
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)
	mockClient := newMockShardAssignmentClientStream()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		err := dispatcher.RegisterForUpdates(mockClient)
		assert.NoError(t, err)
		wg.Done()
	}()

	mockClient.cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())

}

func TestShardAssignmentDispatcher_AddClient(t *testing.T) {
	shard0InitialAssignment := newShardAssignment(0, "server1", 0, 100)
	shard1InitialAssignment := newShardAssignment(1, "server2", 100, math.MaxUint32)
	shard1UpdatedAssignment := newShardAssignment(1, "server3", 100, math.MaxUint32)

	dispatcher := NewShardAssignmentDispatcher()

	coordinatorStream := newMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.ShardAssignment(coordinatorStream)
		assert.NoError(t, err)
	}()

	request := &proto.ShardAssignmentsResponse{
		Assignments: []*proto.ShardAssignment{
			shard0InitialAssignment,
			shard1InitialAssignment,
		},
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	}
	coordinatorStream.AddRequest(request)
	// Wait for the dispatcher to process the initializing request
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	// Should get the whole assignment as they arrived from controller
	mockClient := newMockShardAssignmentClientStream()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := dispatcher.RegisterForUpdates(mockClient)
		assert.NoError(t, err)
		wg.Done()
	}()

	response := mockClient.GetResponse()
	assert.Equal(t, request, response)

	request = &proto.ShardAssignmentsResponse{
		Assignments: []*proto.ShardAssignment{
			shard0InitialAssignment,
			shard1UpdatedAssignment,
		},
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	}
	coordinatorStream.AddRequest(request)

	// Should get the assignment update as they arrived from controller
	response = mockClient.GetResponse()
	assert.Equal(t, request, response)

	mockClient.cancel()
	wg.Wait()

	// Should get the whole assignment with the update applied
	mockClient = newMockShardAssignmentClientStream()
	wg2 := sync.WaitGroup{}
	wg2.Add(1)

	go func() {
		err := dispatcher.RegisterForUpdates(mockClient)
		assert.NoError(t, err)
		wg2.Done()
	}()

	response = mockClient.GetResponse()
	assert.Equal(t, request, response)

	mockClient.cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())
}

func newShardAssignment(id uint32, leader string, min uint32, max uint32) *proto.ShardAssignment {
	return &proto.ShardAssignment{
		ShardId: id,
		Leader:  leader,
		ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
			Int32HashRange: &proto.Int32HashRange{
				MinHashInclusive: min,
				MaxHashInclusive: max,
			},
		},
	}
}

func TestShardGenerator(t *testing.T) {
	assignments := generateShards("localhost", 4)
	assertNext(t, assignments[0], 0, 0, 1073741823)
	assertNext(t, assignments[1], 1, 1073741824, 2147483647)
	assertNext(t, assignments[2], 2, 2147483648, 3221225471)
	assertNext(t, assignments[3], 3, 3221225472, 4294967295)
}

func assertNext(
	t *testing.T,
	assignment *proto.ShardAssignment,
	expectedShardId uint32,
	expectedLowerBound uint32,
	expectedUpperBound uint32) {
	assert.EqualValues(t, expectedShardId, assignment.ShardId)
	rng := assignment.ShardBoundaries.(*proto.ShardAssignment_Int32HashRange).Int32HashRange
	assert.EqualValues(t, expectedLowerBound, rng.MinHashInclusive)
	assert.EqualValues(t, expectedUpperBound, rng.MaxHashInclusive)
}
