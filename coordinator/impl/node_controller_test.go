package impl

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/health/grpc_health_v1"
	"oxia/coordinator/model"
	"oxia/proto"
	"testing"
	"time"
)

func TestNodeController_HealthCheck(t *testing.T) {
	addr := model.ServerAddress{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}

	sap := newMockShardAssignmentsProvider()
	nal := newMockNodeAvailabilityListener()
	rpc := newMockRpcProvider()
	nc := NewNodeController(addr, sap, nal, rpc)

	assert.Equal(t, Running, nc.Status())

	node := rpc.GetNode(addr)
	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.healthClient.SetError(errors.New("failed to connect"))

	unavailableNode = <-nal.events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	assert.NoError(t, nc.Close())
}

func TestNodeController_ShardsAssignments(t *testing.T) {
	addr := model.ServerAddress{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}

	sap := newMockShardAssignmentsProvider()
	nal := newMockNodeAvailabilityListener()
	rpc := newMockRpcProvider()
	nc := NewNodeController(addr, sap, nal, rpc)

	node := rpc.GetNode(addr)

	resp := &proto.ShardAssignmentsResponse{
		Assignments: []*proto.ShardAssignment{{
			ShardId: 0,
			Leader:  "leader-0",
		}, {
			ShardId: 1,
			Leader:  "leader-1",
		}},
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	}

	sap.set(resp)

	update := <-node.shardAssignmentsStream.updates
	assert.Equal(t, resp, update)

	// Simulate 1 single stream send error
	node.shardAssignmentsStream.SetError(errors.New("failed to send"))

	resp2 := &proto.ShardAssignmentsResponse{
		Assignments: []*proto.ShardAssignment{{
			ShardId: 0,
			Leader:  "leader-1",
		}, {
			ShardId: 1,
			Leader:  "leader-2",
		}},
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	}

	sap.set(resp2)

	update = <-node.shardAssignmentsStream.updates
	assert.Equal(t, resp2, update)

	assert.NoError(t, nc.Close())
}
