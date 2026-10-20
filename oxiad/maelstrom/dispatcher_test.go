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
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/oxia-db/oxia/common/proto"
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
		deliver(t, d, test.request)
		forwarded := decodeMessage(t, receiveLines(t, lines, 1)[0])
		require.Equal(t, "n3", forwarded.Dest)

		deliver(t, d, fmt.Sprintf(test.reply, forwarded.Body.MsgId))
		reply := decodeMessage(t, receiveLines(t, lines, 1)[0])
		assert.Equal(t, "c1", reply.Dest)
		assert.Equal(t, request.Body.MsgId, *reply.Body.InReplyTo)
		assert.Empty(t, d.proxiedRequests)
	}
}

func TestDispatcherProxyToLeaderFailsOnUnsupportedType(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	d.currentLeader = "n3"

	err := d.proxyToLeader(MsgTypeWriteOk, &Message[BaseMessageBody]{Src: "c1"})

	require.ErrorContains(t, err, "unknown client request type write_ok")
	require.Empty(t, d.proxiedRequests)
}

func TestDispatcherReceivedMessageServesOxiaRequests(t *testing.T) {
	lines := captureStdout(t)

	dataServer := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	deliver(t, dataServer, marshalLine(t, &Message[OxiaMessage]{
		Src: "n1",
		Body: OxiaMessage{
			BaseMessageBody: BaseMessageBody{Type: MsgTypeHealthCheck, MsgId: 5},
			OxiaMsg:         json.RawMessage(`{}`),
		},
	}))

	msgType, msg, processId := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeHealthCheckOk, msgType)
	require.Equal(t, "n1", msg.(*Message[OxiaMessage]).Dest)
	require.Equal(t, int64(5), *msg.(*Message[OxiaMessage]).Body.InReplyTo)
	require.Equal(t, dataServer.grpcProvider.processId, processId.(*wrapperspb.Int64Value).GetValue())
}

func TestDispatcherReceivedMessageRoutesReplicateStreams(t *testing.T) {
	lines := captureStdout(t)

	leader := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	replication := newRecordingReplicationServer()
	follower := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	follower.grpcProvider.RegisterService(&proto.OxiaLogReplication_ServiceDesc, replication)

	client, err := leader.replicationProvider.GetReplicateStream(context.Background(), "n2", "default", 0, 1)
	require.NoError(t, err)
	streamId := client.(*maelstromReplicateClient).streamId

	// The append reaches the follower's Replicate handler
	require.NoError(t, client.Send(entryAppend(0)))
	deliver(t, follower, receiveLines(t, lines, 1)[0])
	require.Equal(t, int64(0), <-replication.offsets)

	// The ack of the follower reaches the leader's stream
	serverStream := newMaelstromReplicateServerStream(appendMessage(thisNode, streamId, 0))
	require.NoError(t, serverStream.Send(&proto.Ack{Offset: 0}))
	deliver(t, leader, receiveLines(t, lines, 1)[0])
	ack, err := client.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(0), ack.Offset)

	// The leader resets the stream: the follower stops its handler, and tells
	// the leader back, which fails its side of the stream
	sendStreamError("n2", streamId)
	deliver(t, follower, receiveLines(t, lines, 1)[0])
	require.ErrorIs(t, <-replication.done, io.EOF)
	deliver(t, leader, receiveLines(t, lines, 1)[0])
	_, err = client.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestDispatcherOnOxiaStreamRequestMessageFailsOnUnknownType(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())

	err := d.onOxiaStreamRequestMessage(MsgTypeInit, &Message[OxiaStreamMessage]{}, nil)

	require.ErrorContains(t, err, "unknown stream message type init")
}

func TestDispatcherReceivedMessageRejectsClientRequestsWithoutLeader(t *testing.T) {
	lines := captureStdout(t)

	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	for _, request := range []any{writeMessage(1, 2), readMessage(1), casMessage(1, 2, 3)} {
		deliver(t, d, marshalLine(t, request))

		msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
		require.Equal(t, MsgTypeError, msgType)
		res := msg.(*Message[ErrorResponse])
		require.Equal(t, "c1", res.Dest)
		require.Equal(t, 11, res.Body.Code)
		requestType, parsed, _ := parseLine(t, marshalLine(t, request))
		requestId, _, _ := requireOrigin(t, requestType, parsed)
		require.Equal(t, requestId, *res.Body.InReplyTo)
	}
}

func TestDispatcherReceivedMessageServesClientRequestsAsLeader(t *testing.T) {
	setThisNode(t, "n2")
	lines := captureStdout(t)

	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	d.grpcProvider.RegisterService(&proto.OxiaClient_ServiceDesc, newKVClientServer(map[string]string{}))
	d.currentLeader = "n2"

	deliver(t, d, marshalLine(t, writeMessage(1, 2)))
	msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeWriteOk, msgType)
	require.Equal(t, "n2", msg.(*Message[BaseMessageBody]).Src)
	require.Equal(t, "c1", msg.(*Message[BaseMessageBody]).Dest)
}

func TestDispatcherReceivedMessageProxiesClientRequestsToLeader(t *testing.T) {
	setThisNode(t, "n2")
	lines := captureStdout(t)

	errorCode := 22
	tests := []struct {
		name     string
		request  any
		response func(proxyId int64) any
		wantType MsgType
	}{
		{"write", writeMessage(1, 2), func(proxyId int64) any {
			return &Message[BaseMessageBody]{Src: "n3", Body: BaseMessageBody{Type: MsgTypeWriteOk, InReplyTo: &proxyId}}
		}, MsgTypeWriteOk},
		{"cas", casMessage(1, 2, 3), func(proxyId int64) any {
			return &Message[BaseMessageBody]{Src: "n3", Body: BaseMessageBody{Type: MsgTypeCasOk, InReplyTo: &proxyId}}
		}, MsgTypeCasOk},
		{"read", readMessage(1), func(proxyId int64) any {
			return &Message[ReadResponse]{Src: "n3", Body: ReadResponse{
				BaseMessageBody: BaseMessageBody{Type: MsgTypeReadOk, InReplyTo: &proxyId},
				Value:           7,
			}}
		}, MsgTypeReadOk},
		{"error", casMessage(1, 2, 3), func(proxyId int64) any {
			return &Message[ErrorResponse]{Src: "n3", Body: ErrorResponse{
				BaseMessageBody: BaseMessageBody{Type: MsgTypeError, InReplyTo: &proxyId},
				Code:            errorCode,
				Text:            "precondition-failed",
			}}
		}, MsgTypeError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
			d.currentLeader = "n3"
			requestType, request, _ := parseLine(t, marshalLine(t, tt.request))
			requestId, _, _ := requireOrigin(t, requestType, request)

			// The request goes to the leader, under a new id
			deliver(t, d, marshalLine(t, tt.request))
			proxiedType, proxied, _ := parseLine(t, receiveLines(t, lines, 1)[0])
			require.Equal(t, requestType, proxiedType)
			proxyId, _, _ := requireOrigin(t, proxiedType, proxied)
			require.Equal(t, "n3", destination(proxied))

			// The answer of the leader goes back to the client
			deliver(t, d, marshalLine(t, tt.response(proxyId)))
			msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
			require.Equal(t, tt.wantType, msgType)
			require.Equal(t, "c1", destination(msg))
			_, inReplyTo, _ := requireOrigin(t, msgType, msg)
			require.Equal(t, requestId, inReplyTo)
			switch res := msg.(type) {
			case *Message[ReadResponse]:
				require.Equal(t, int64(7), res.Body.Value)
			case *Message[ErrorResponse]:
				require.Equal(t, errorCode, res.Body.Code)
				require.Equal(t, "precondition-failed", res.Body.Text)
			default:
				// write_ok and cas_ok carry no payload
			}
		})
	}
}

func TestDispatcherReceivedMessageFailsOnUnknownType(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())

	err := d.ReceivedMessage(parseLine(t, `{"src":"c1","dest":"n2","body":{"type":"unknown","msg_id":1}}`))

	require.ErrorContains(t, err, "unknown message type unknown")
}

func TestDispatcherReceivedMessageFailsOnInvalidResponseId(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())

	err := d.ReceivedMessage(parseLine(t, `{"src":"n2","body":{"type":"health-ok","in_reply_to":99,"m":0}}`))

	require.ErrorContains(t, err, "invalid response id 99")
}

func TestDispatcherRpcRequestFailsOnUnserializableMessage(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())

	_, err := d.RpcRequest(t.Context(), "n2", MsgTypeNewTermRequest, &proto.NewTermRequest{Namespace: "\xff"})

	require.ErrorContains(t, err, "failed to serialize proto to json")
	require.Empty(t, d.requests)
}

func TestDispatcherRpcRequestFailsOnClosedStdout(t *testing.T) {
	closeStdout(t)
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())

	_, err := d.RpcRequest(t.Context(), "n2", MsgTypeNewTermRequest, &proto.NewTermRequest{})

	require.ErrorContains(t, err, "failed to write the message")
	require.Empty(t, d.requests)
}

func TestDispatcherRpcRequestTimesOutWithoutResponse(t *testing.T) {
	previousTimeout := rpcRequestTimeout
	rpcRequestTimeout = 10 * time.Millisecond
	t.Cleanup(func() { rpcRequestTimeout = previousTimeout })
	lines := captureStdout(t)
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())

	_, err := d.RpcRequest(t.Context(), "n2", MsgTypeNewTermRequest, &proto.NewTermRequest{})

	require.ErrorContains(t, err, "timed out")
	require.Equal(t, "n2", decodeMessage(t, receiveLines(t, lines, 1)[0]).Dest)
}

func TestGetOriginReadsInReplyTo(t *testing.T) {
	inReplyTo := int64(4)
	base := BaseMessageBody{MsgId: 3, InReplyTo: &inReplyTo}
	tests := []struct {
		msgType MsgType
		msg     any
	}{
		{MsgTypeWrite, &Message[Write]{Src: "c1", Body: Write{BaseMessageBody: base}}},
		{MsgTypeRead, &Message[Read]{Src: "c1", Body: Read{BaseMessageBody: base}}},
		{MsgTypeCas, &Message[Cas]{Src: "c1", Body: Cas{BaseMessageBody: base}}},
	}
	for _, tt := range tests {
		msgId, gotInReplyTo, origin := requireOrigin(t, tt.msgType, tt.msg)
		require.Equal(t, int64(3), msgId)
		require.Equal(t, inReplyTo, gotInReplyTo)
		require.Equal(t, "c1", origin)
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

func marshalLine(t *testing.T, msg any) string {
	t.Helper()
	b, err := json.Marshal(msg)
	require.NoError(t, err)
	return string(b)
}

func destination(msg any) string {
	switch m := msg.(type) {
	case *Message[Write]:
		return m.Dest
	case *Message[Read]:
		return m.Dest
	case *Message[Cas]:
		return m.Dest
	case *Message[BaseMessageBody]:
		return m.Dest
	case *Message[ReadResponse]:
		return m.Dest
	case *Message[ErrorResponse]:
		return m.Dest
	default:
		panic("unexpected message")
	}
}

func requireOrigin(t *testing.T, msgType MsgType, m any) (msgId int64, inReplyTo int64, origin string) {
	t.Helper()
	msgId, inReplyTo, origin, err := getOrigin(msgType, m)
	require.NoError(t, err)
	return msgId, inReplyTo, origin
}

// setThisNode makes the test run as the node id, which the messages it sends
// carry as their source.
func setThisNode(t *testing.T, node string) {
	t.Helper()
	previous := thisNode
	thisNode = node
	t.Cleanup(func() { thisNode = previous })
}

// failProtoMarshal makes the serialization of every proto message fail.
func failProtoMarshal(t *testing.T) {
	t.Helper()
	previousMarshal := protoMarshal
	protoMarshal = func(pb.Message) ([]byte, error) { return nil, errors.New("marshal failed") }
	t.Cleanup(func() { protoMarshal = previousMarshal })
}
