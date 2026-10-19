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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDispatcherApplyShardAssignmentsSkipsStaleAssignments(t *testing.T) {
	d := &dispatcher{}
	d.applyShardAssignments(assignmentsMessage(1, 12), assignmentsWithLeader("n3"))

	// A resend of the previous assignments, that the network delayed past the
	// new ones
	d.applyShardAssignments(assignmentsMessage(1, 10), assignmentsWithLeader("n4"))
	require.Equal(t, "n3", d.currentLeader)

	// The messages of a new stream are not compared with the old stream's
	d.applyShardAssignments(assignmentsMessage(2, 3), assignmentsWithLeader("n4"))
	require.Equal(t, "n4", d.currentLeader)
}

// A node that doesn't lead the shard forwards the client requests to the
// leader, and the leader's replies back to the client.
func TestDispatcherProxyToLeaderRepliesToClient(t *testing.T) {
	lines := captureStdout(t)
	previousNode := thisNode
	thisNode = "n2"
	t.Cleanup(func() { thisNode = previousNode })

	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	d.currentLeader = "n3"

	for _, test := range []struct {
		request string
		reply   string
	}{
		{`{"src":"c1","dest":"n2","body":{"type":"write","msg_id":7,"key":1,"value":2}}`,
			`{"src":"n3","dest":"n2","body":{"type":"write_ok","in_reply_to":%d}}`},
		{`{"src":"c1","dest":"n2","body":{"type":"read","msg_id":8,"key":1}}`,
			`{"src":"n3","dest":"n2","body":{"type":"read_ok","in_reply_to":%d,"value":2}}`},
	} {
		request := decodeMessage(t, test.request)
		deliver(d, test.request)
		forwarded := decodeMessage(t, receiveLines(t, lines, 1)[0])
		require.Equal(t, "n3", forwarded.Dest)

		deliver(d, fmt.Sprintf(test.reply, forwarded.Body.MsgId))
		reply := decodeMessage(t, receiveLines(t, lines, 1)[0])
		assert.Equal(t, "c1", reply.Dest)
		assert.Equal(t, request.Body.MsgId, *reply.Body.InReplyTo)
		assert.Empty(t, d.proxiedRequests)
	}
}

func assignmentsMessage(streamId int64, msgId int64) *Message[OxiaStreamMessage] {
	return &Message[OxiaStreamMessage]{
		Src: "n1",
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{Type: MsgTypeShardAssignmentsResponse, MsgId: msgId},
			StreamId:        streamId,
		},
	}
}

func decodeMessage(t *testing.T, line string) *Message[BaseMessageBody] {
	t.Helper()
	m := &Message[BaseMessageBody]{}
	require.NoError(t, json.Unmarshal([]byte(line), m))
	return m
}
