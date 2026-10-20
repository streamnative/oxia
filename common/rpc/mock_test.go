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

package rpc

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/common/proto"
)

func TestMockServerReplicateStreamSendAfterCancel(t *testing.T) {
	stream := NewMockServerReplicateStream()
	// With the responses buffer full, only the cancellation can unblock Send
	for range cap(stream.Responses) {
		require.NoError(t, stream.Send(&proto.Ack{}))
	}
	stream.Cancel()

	assert.ErrorIs(t, stream.Send(&proto.Ack{}), context.Canceled)
}

func TestMockRpcClientCloseCancelsStreams(t *testing.T) {
	client := NewMockRpcClient()
	stream, err := client.GetReplicateStream(context.Background(), "", "", 0, 0)
	require.NoError(t, err)

	require.NoError(t, client.Close())

	_, err = stream.Recv()
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMockReplicateStreamSendAfterCloseSend(t *testing.T) {
	stream := NewMockReplicateStream(context.Background(), make(chan *proto.Append), make(chan *proto.Ack))
	require.NoError(t, stream.CloseSend())

	assert.ErrorIs(t, stream.Send(&proto.Append{}), context.Canceled)
}

func TestMockReplicateStreamRecvClosedChannel(t *testing.T) {
	recv := make(chan *proto.Ack)
	close(recv)
	stream := NewMockReplicateStream(context.Background(), make(chan *proto.Append), recv)

	_, err := stream.Recv()
	assert.ErrorIs(t, err, io.EOF)
}

func TestMockReplicateStreamCloseSendWithoutContext(t *testing.T) {
	stream := &MockReplicateStream{}
	assert.NoError(t, stream.CloseSend())
}

func TestMockShardAssignmentControllerStreamSendAndClose(t *testing.T) {
	stream := NewMockShardAssignmentControllerStream()
	response := &proto.CoordinationShardAssignmentsResponse{}

	require.NoError(t, stream.SendAndClose(response))
	assert.Same(t, response, stream.GetResponse())
}

func TestMockServerSendSnapshotStreamSendAndClose(t *testing.T) {
	stream := NewMockServerSendSnapshotStream()
	response := &proto.SnapshotResponse{AckOffset: 5}

	require.NoError(t, stream.SendAndClose(response))
	assert.Same(t, response, stream.GetResponse())
}

func TestMockSendSnapshotClientStreamSendAfterCloseSend(t *testing.T) {
	stream := NewMockSendSnapshotClientStream(context.Background())
	// With the requests buffer full, only the cancellation can unblock Send
	for range cap(stream.Requests) {
		require.NoError(t, stream.Send(&proto.SnapshotChunk{}))
	}
	require.NoError(t, stream.CloseSend())

	assert.ErrorIs(t, stream.Send(&proto.SnapshotChunk{}), context.Canceled)
}

func TestMockBaseUnimplementedMethodsPanic(t *testing.T) {
	base := &MockBase{}
	assert.Panics(t, func() { _ = base.SendHeader(metadata.MD{}) })
	assert.Panics(t, func() { _ = base.RecvMsg(nil) })
	assert.Panics(t, func() { _ = base.SendMsg(nil) })
	assert.Panics(t, func() { base.SetTrailer(metadata.MD{}) })
	assert.Panics(t, func() { _, _ = base.Header() })
	assert.Panics(t, func() { _ = base.Trailer() })
}

func TestMockBaseSetHeader(t *testing.T) {
	base := &MockBase{}
	md := metadata.Pairs("key", "value")

	require.NoError(t, base.SetHeader(md))
	assert.Equal(t, md, base.md)
}

func TestMockBaseContextDefaultsToBackground(t *testing.T) {
	assert.Equal(t, context.Background(), (&MockBase{}).Context())
}
