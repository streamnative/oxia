// Copyright 2023-2026 The Oxia Authors
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

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/health"
)

func TestMaelstromShardAssignmentClientSendResendsUntilAcked(t *testing.T) {
	previousInterval := assignmentsResendInterval
	assignmentsResendInterval = 50 * time.Millisecond
	t.Cleanup(func() { assignmentsResendInterval = previousInterval })

	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	stream, err := newRpcProvider(coordinator).PushShardAssignments(t.Context(), &proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)
	require.NoError(t, stream.Send(assignmentsWithLeader("n4")))

	// The push is lost in a partition: the stream sends it again
	push := receiveLines(t, lines, 2)
	require.Equal(t, push[0], push[1])

	// The data server gets it, and acks it
	dataServer := &dispatcher{}
	deliver(dataServer, push[1])
	require.Equal(t, "n4", dataServer.currentLeader)
	ack := ""
	for ack == "" {
		line := receiveLines(t, lines, 1)[0]
		if msgType, _, _ := parseRequest(line); msgType == MsgTypeShardAssignmentsAck {
			ack = line
		} else {
			// A resend that went out before the ack
			require.Equal(t, push[0], line)
		}
	}

	// The ack stops the resends
	deliver(coordinator, ack)
	fmt.Fprintln(os.Stdout, "acked")
	for line := receiveLines(t, lines, 1)[0]; line != "acked"; line = receiveLines(t, lines, 1)[0] {
		require.Equal(t, push[0], line)
	}
	select {
	case line := <-lines:
		require.Failf(t, "the stream resent the acked assignments", "message: %s", line)
	case <-time.After(5 * assignmentsResendInterval):
	}
}

func TestMaelstromHealthCheckClientCheckClosesAssignmentStreamsAtFailureThreshold(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	provider := newRpcProvider(coordinator)
	node := &proto.DataServerIdentity{Internal: "n4"}
	stream, err := provider.PushShardAssignments(t.Context(), node)
	require.NoError(t, err)
	healthClient, err := provider.GetHealthClient(node)
	require.NoError(t, err)
	dataServer := newMaelstromGrpcProvider()

	// The probes lost below the threshold keep the stream, and a probe that
	// gets answered starts the count again
	answerProbe(t, lines, coordinator, healthClient, dataServer)
	for range health.DefaultCheckPolicy.FailureThreshold - 1 {
		loseProbe(t, lines, healthClient)
	}
	answerProbe(t, lines, coordinator, healthClient, dataServer)
	for range health.DefaultCheckPolicy.FailureThreshold - 1 {
		loseProbe(t, lines, healthClient)
	}
	require.NoError(t, stream.Context().Err())

	loseProbe(t, lines, healthClient)
	require.ErrorIs(t, stream.Context().Err(), context.Canceled)
}

func TestMaelstromHealthCheckClientCheckClosesAssignmentStreamsOfRestartedDataServer(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	provider := newRpcProvider(coordinator)
	node := &proto.DataServerIdentity{Internal: "n4"}
	stream, err := provider.PushShardAssignments(t.Context(), node)
	require.NoError(t, err)
	healthClient, err := provider.GetHealthClient(node)
	require.NoError(t, err)
	dataServer := newMaelstromGrpcProvider()

	answerProbe(t, lines, coordinator, healthClient, dataServer)
	answerProbe(t, lines, coordinator, healthClient, dataServer)
	require.NoError(t, stream.Context().Err())

	// The kill nemesis restarts the data server, before any probe is lost
	restarted := newMaelstromGrpcProvider()
	restarted.processId = dataServer.processId + 1
	answerProbe(t, lines, coordinator, healthClient, restarted)
	require.ErrorIs(t, stream.Context().Err(), context.Canceled)

	// The coordinator re-opens the stream, and the new process keeps it
	stream, err = provider.PushShardAssignments(t.Context(), node)
	require.NoError(t, err)
	answerProbe(t, lines, coordinator, healthClient, restarted)
	require.NoError(t, stream.Context().Err())
}

func TestMaelstromCoordinatorRpcProviderDeleteShard(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	dataServer := newMaelstromGrpcProvider()
	deleted := make(chan *proto.DeleteShardRequest, 1)
	dataServer.RegisterService(&proto.OxiaCoordination_ServiceDesc, &deleteShardServer{deleted: deleted})

	res := make(chan error, 1)
	go func() {
		_, err := newRpcProvider(coordinator).DeleteShard(t.Context(), &proto.DataServerIdentity{Internal: "n4"},
			&proto.DeleteShardRequest{Namespace: constant.DefaultNamespace, Shard: 1, Term: 2})
		res <- err
	}()

	msgType, request, protoMsg := parseRequest(receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeDeleteShardRequest, msgType)
	dataServer.HandleOxiaRequest(msgType, request.(*Message[OxiaMessage]), protoMsg)
	deliver(coordinator, receiveLines(t, lines, 1)[0])
	require.NoError(t, <-res)
	require.EqualValues(t, 1, (<-deleted).Shard)
}

// deliver hands a Maelstrom message to the dispatcher, as the main loop does
// with each line it reads.
func deliver(d *dispatcher, line string) {
	d.ReceivedMessage(parseRequest(line))
}

func assignmentsWithLeader(leader string) *proto.ShardAssignments {
	return &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {Assignments: []*proto.ShardAssignment{{Leader: leader}}},
		},
	}
}

// answerProbe runs a health check of the coordinator, and answers it from the
// data server.
func answerProbe(t *testing.T, lines <-chan string, coordinator *dispatcher,
	healthClient grpc_health_v1.HealthClient, dataServer *maelstromGrpcProvider) {
	t.Helper()
	checked := make(chan error, 1)
	go func() {
		_, err := healthClient.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		checked <- err
	}()

	msgType, probe, _ := parseRequest(receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeHealthCheck, msgType)
	dataServer.HandleOxiaRequest(msgType, probe.(*Message[OxiaMessage]), nil)
	deliver(coordinator, receiveLines(t, lines, 1)[0])
	require.NoError(t, <-checked)
}

// loseProbe runs a health check of the coordinator that gets no answer, as
// across a partition.
func loseProbe(t *testing.T, lines <-chan string, healthClient grpc_health_v1.HealthClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	_, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	receiveLines(t, lines, 1)
}

// deleteShardServer is a data server that records the shards it deletes.
type deleteShardServer struct {
	proto.UnimplementedOxiaCoordinationServer

	deleted chan *proto.DeleteShardRequest
}

func (s *deleteShardServer) DeleteShard(_ context.Context, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	s.deleted <- req
	return &proto.DeleteShardResponse{}, nil
}
