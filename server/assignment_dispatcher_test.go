package server

import (
	"github.com/stretchr/testify/assert"
	"math"
	"oxia/proto"
	"testing"
	"time"
)

func TestUninitializedAssignmentDispatcher(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher()
	mockClient := &mockServerStream[any, *proto.ShardAssignmentsResponse]{}
	assert.False(t, dispatcher.Initialized())
	err := dispatcher.AddClient(mockClient)
	assert.Error(t, err)
}

func TestShardAssignmentDispatcher_Initialized(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher()
	defer assert.NoError(t, dispatcher.Close())
	coordinatorStream := &mockServerStream[*proto.CoordinationShardAssignmentRequest, *proto.CoordinationShardAssignmentResponse]{}
	go dispatcher.ShardAssignment(coordinatorStream)
	assert.False(t, dispatcher.Initialized())
	coordinatorStream.AddRequest(&proto.CoordinationShardAssignmentRequest{
		Assignments: []*proto.CoordinationShardAssignment{
			{
				ShardId: 0,
				Leader:  "server1",
				ShardBoundaries: &proto.CoordinationShardAssignment_Int32HashRange{
					Int32HashRange: &proto.CoordinationInt32HashRange{
						MinHashInclusive: 0,
						MaxHashExclusive: 100,
					},
				},
			}, {
				ShardId: 82,
				Leader:  "server2",
				ShardBoundaries: &proto.CoordinationShardAssignment_Int32HashRange{
					Int32HashRange: &proto.CoordinationInt32HashRange{
						MinHashInclusive: 100,
						MaxHashExclusive: math.MaxUint32,
					},
				},
			},
		},
		ShardKeyRouter: proto.CoordinationShardKeyRouter_XXHASH3,
	})
	assert.Eventually(t, func() bool { return dispatcher.Initialized() }, 1*time.Second, 10*time.Millisecond)
	mockClient := &mockServerStream[any, *proto.ShardAssignmentsResponse]{}
	err := dispatcher.AddClient(mockClient)
	assert.NoError(t, err)
}
