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
	"testing"

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

func assignmentsMessage(streamId int64, msgId int64) *Message[OxiaStreamMessage] {
	return &Message[OxiaStreamMessage]{
		Src: "n1",
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{Type: MsgTypeShardAssignmentsResponse, MsgId: msgId},
			StreamId:        streamId,
		},
	}
}
