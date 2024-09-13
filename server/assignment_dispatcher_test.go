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
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/container"
	"github.com/streamnative/oxia/proto"
)

func TestUninitializedAssignmentDispatcher(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(health.NewServer())
	mockClient := newMockShardAssignmentClientStream()
	assert.False(t, dispatcher.Initialized())
	req := &proto.ShardAssignmentsRequest{Namespace: common.DefaultNamespace}
	err := dispatcher.RegisterForUpdates(req, mockClient)
	assert.ErrorIs(t, err, common.ErrorNotInitialized)
	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_Initialized(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(health.NewServer())
	coordinatorStream := newMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	assert.False(t, dispatcher.Initialized())
	coordinatorStream.AddRequest(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			common.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1", 0, 100),
					newShardAssignment(1, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	})
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)
	mockClient := newMockShardAssignmentClientStream()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		req := &proto.ShardAssignmentsRequest{Namespace: common.DefaultNamespace}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
		wg.Done()
	}()

	mockClient.cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_ReadinessProbe(t *testing.T) {
	healthServer := health.NewServer()
	dispatcher := NewShardAssignmentDispatcher(healthServer)
	coordinatorStream := newMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	assert.False(t, dispatcher.Initialized())

	// Readiness probe should fail while not initialized
	resp, err := healthServer.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: container.ReadinessProbeService,
	})

	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Nil(t, resp)

	coordinatorStream.AddRequest(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			common.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1", 0, 100),
					newShardAssignment(1, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	})
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	resp, err = healthServer.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: container.ReadinessProbeService,
	})
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_AddClient(t *testing.T) {
	shard0InitialAssignment := newShardAssignment(0, "server1", 0, 100)
	shard1InitialAssignment := newShardAssignment(1, "server2", 100, math.MaxUint32)
	shard1UpdatedAssignment := newShardAssignment(1, "server3", 100, math.MaxUint32)

	dispatcher := NewShardAssignmentDispatcher(health.NewServer())

	coordinatorStream := newMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	request := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			common.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					shard0InitialAssignment,
					shard1InitialAssignment,
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
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
		req := &proto.ShardAssignmentsRequest{Namespace: common.DefaultNamespace}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
		wg.Done()
	}()

	response := mockClient.GetResponse()
	assert.Equal(t, request, response)

	request = &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			common.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					shard0InitialAssignment,
					shard1UpdatedAssignment,
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
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
		req := &proto.ShardAssignmentsRequest{Namespace: common.DefaultNamespace}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
		wg2.Done()
	}()

	response = mockClient.GetResponse()
	assert.Equal(t, request, response)

	mockClient.cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_MultipleNamespaces(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(health.NewServer())

	coordinatorStream := newMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	request := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			"default": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server0", 0, 100),
					newShardAssignment(1, "server1", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
			"test-ns-1": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(2, "server1", 0, 100),
					newShardAssignment(3, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
			"test-ns-2": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(4, "server3", 0, 100),
					newShardAssignment(5, "server4", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
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
		req := &proto.ShardAssignmentsRequest{Namespace: "test-ns-1"}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
		wg.Done()
	}()

	mockClient.cancel()
	wg.Wait()

	response := mockClient.GetResponse()
	assert.Equal(t, &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			"test-ns-1": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(2, "server1", 0, 100),
					newShardAssignment(3, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}, response)

	// If namespace is not passed, it will use "default"
	mockClient = newMockShardAssignmentClientStream()
	wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		req := &proto.ShardAssignmentsRequest{Namespace: ""}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
		wg.Done()
	}()

	mockClient.cancel()
	wg.Wait()

	response = mockClient.GetResponse()
	assert.Equal(t, &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			"default": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server0", 0, 100),
					newShardAssignment(1, "server1", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}, response)

	// If the namespace is not valid, we'll get an error
	mockClient = newMockShardAssignmentClientStream()
	wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		req := &proto.ShardAssignmentsRequest{Namespace: "non-valid-namespace"}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.ErrorIs(t, err, common.ErrorNamespaceNotFound)
		wg.Done()
	}()

	mockClient.cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())
}

func newShardAssignment(id int64, leader string, min uint32, max uint32) *proto.ShardAssignment {
	return &proto.ShardAssignment{
		Shard:  id,
		Leader: leader,
		ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
			Int32HashRange: &proto.Int32HashRange{
				MinHashInclusive: min,
				MaxHashInclusive: max,
			},
		},
	}
}
