// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/stretchr/testify/assert"
	"math"
	"oxia/common"
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
	assert.ErrorIs(t, err, common.ErrorNotInitialized)
	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_Initialized(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher()
	coordinatorStream := newMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	assert.False(t, dispatcher.Initialized())
	coordinatorStream.AddRequest(&proto.ShardAssignments{
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
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	request := &proto.ShardAssignments{
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

	request = &proto.ShardAssignments{
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
