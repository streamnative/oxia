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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
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
