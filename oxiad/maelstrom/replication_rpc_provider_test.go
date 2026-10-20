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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

func TestMaelstromReplicationRpcProviderGetReplicateStreamNotifiesFollowerOnClose(t *testing.T) {
	lines := captureStdout(t)

	provider := newMaelstromReplicationRpcProvider()
	ctx, cancel := context.WithCancel(t.Context())
	client, err := provider.GetReplicateStream(ctx, "n2", "default", 0, 1)
	require.NoError(t, err)
	streamId := client.(*maelstromReplicateClient).streamId

	header, err := client.Header()
	require.NoError(t, err)
	require.Empty(t, header)

	// The follower cursor tears the stream down
	cancel()
	requireStreamError(t, receiveLines(t, lines, 1)[0], "n2", streamId)
	_, err = client.Recv()
	require.ErrorIs(t, err, context.Canceled)

	// The acks that arrive late are dropped
	provider.HandleAck(streamId, &proto.Ack{Offset: 1})
	require.Empty(t, client.(*maelstromReplicateClient).responses)
	require.NoError(t, provider.Close())
}

func TestMaelstromReplicationRpcProviderHandleAckDeliversToStream(t *testing.T) {
	provider := newMaelstromReplicationRpcProvider()
	client, err := provider.GetReplicateStream(context.Background(), "n2", "default", 0, 1)
	require.NoError(t, err)
	streamId := client.(*maelstromReplicateClient).streamId

	provider.HandleAck(streamId, &proto.Ack{Offset: 3})
	ack, err := client.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(3), ack.Offset)

	// Acks are cumulative: the ones that overflow the buffer are dropped,
	// without blocking the dispatcher
	responses := client.(*maelstromReplicateClient).responses
	for offset := range int64(cap(responses) + 1) {
		provider.HandleAck(streamId, &proto.Ack{Offset: offset})
	}
	require.Len(t, responses, cap(responses))
}

func TestMaelstromReplicationRpcProviderHandleStreamErrorFailsStream(t *testing.T) {
	provider := newMaelstromReplicationRpcProvider()
	client, err := provider.GetReplicateStream(context.Background(), "n2", "default", 0, 1)
	require.NoError(t, err)
	streamId := client.(*maelstromReplicateClient).streamId

	// Unknown streams are ignored
	provider.HandleStreamError(streamId + 1000)

	provider.HandleStreamError(streamId)
	_, err = client.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))

	// The stream is gone: a repeated notification is ignored
	provider.HandleStreamError(streamId)
	require.Empty(t, client.(*maelstromReplicateClient).failed)
}

func TestMaelstromReplicationRpcProviderTruncateForwardsToFollower(t *testing.T) {
	lines := captureStdout(t)

	leader := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	headEntry := &proto.EntryId{Term: 2, Offset: 5}
	replication := newRecordingReplicationServer()
	replication.truncate = func(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
		return &proto.TruncateResponse{HeadEntryId: req.HeadEntryId}, nil
	}
	follower := newMaelstromGrpcProvider()
	follower.RegisterService(&proto.OxiaLogReplication_ServiceDesc, replication)

	done := make(chan *proto.TruncateResponse, 1)
	go func() {
		res, err := leader.replicationProvider.Truncate("n2", &proto.TruncateRequest{Term: 2, HeadEntryId: headEntry})
		if err != nil {
			res = nil
		}
		done <- res
	}()
	answerRequest(t, lines, leader, follower)
	require.Equal(t, headEntry.Offset, (<-done).GetHeadEntryId().GetOffset())
}

func TestMaelstromReplicationRpcProviderTruncateReturnsFollowerError(t *testing.T) {
	lines := captureStdout(t)

	leader := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	follower := newMaelstromGrpcProvider()
	follower.RegisterService(&proto.OxiaLogReplication_ServiceDesc, newRecordingReplicationServer())

	done := make(chan error, 1)
	go func() {
		_, err := leader.replicationProvider.Truncate("n2", &proto.TruncateRequest{Term: 2})
		done <- err
	}()
	answerRequest(t, lines, leader, follower)
	require.ErrorContains(t, <-done, "not implemented")
}

func TestMaelstromReplicationRpcProviderSendSnapshotIsNotImplemented(t *testing.T) {
	require.Panics(t, func() {
		_, _ = newMaelstromReplicationRpcProvider().SendSnapshot(t.Context(), "n2", "default", 0, 1)
	})
}

func TestMaelstromReplicateClientSendFailsOnUnserializableAppend(t *testing.T) {
	failProtoMarshal(t)
	client := &maelstromReplicateClient{follower: "n2", streamId: 1}

	require.ErrorContains(t, client.Send(entryAppend(0)), "failed to serialize proto to json")
	require.Zero(t, client.nextSeq)
}

func TestMaelstromReplicateClientSendFailsOnClosedStdout(t *testing.T) {
	closeStdout(t)
	client := &maelstromReplicateClient{follower: "n2", streamId: 1}

	require.ErrorContains(t, client.Send(entryAppend(0)), "failed to write the message")
	require.Zero(t, client.nextSeq)
}
