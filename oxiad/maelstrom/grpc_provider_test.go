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
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/oxia-db/oxia/common/proto"
)

func TestMaelstromGrpcProviderStartGrpcServerRegistersServices(t *testing.T) {
	provider := newMaelstromGrpcProvider()
	coordination := &fakeCoordinationServer{}

	server, err := provider.StartGrpcServer("internal", "localhost:0", func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaCoordinationServer(registrar, coordination)
	}, nil, nil, nil)
	require.NoError(t, err)

	require.Same(t, coordination, provider.getService(oxiaCoordination))
	require.Equal(t, 0, server.Port())
	require.NoError(t, server.Close())
}

func TestMaelstromGrpcProviderGetServiceReturnsNilForUnregisteredService(t *testing.T) {
	require.Nil(t, newMaelstromGrpcProvider().getService(oxiaClient))
}

func TestMaelstromGrpcProviderHandleOxiaRequestRepliesWithResponse(t *testing.T) {
	lines := captureStdout(t)

	headEntry := &proto.EntryId{Term: 2, Offset: 7}
	provider := newMaelstromGrpcProvider()
	provider.RegisterService(&proto.OxiaCoordination_ServiceDesc, &fakeCoordinationServer{
		newTerm: func(*proto.NewTermRequest) (*proto.NewTermResponse, error) {
			return &proto.NewTermResponse{HeadEntryId: headEntry}, nil
		},
		becomeLeader: func(*proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
			return &proto.BecomeLeaderResponse{}, nil
		},
		addFollower: func(*proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
			return &proto.AddFollowerResponse{}, nil
		},
		getStatus: func(*proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
			return &proto.GetStatusResponse{Term: 2, Status: proto.ServingStatus_LEADER}, nil
		},
	})
	replication := newRecordingReplicationServer()
	replication.truncate = func(*proto.TruncateRequest) (*proto.TruncateResponse, error) {
		return &proto.TruncateResponse{HeadEntryId: headEntry}, nil
	}
	provider.RegisterService(&proto.OxiaLogReplication_ServiceDesc, replication)

	tests := []struct {
		msgType  MsgType
		request  pb.Message
		respType MsgType
		response pb.Message
	}{
		{MsgTypeNewTermRequest, &proto.NewTermRequest{}, MsgTypeNewTermResponse,
			&proto.NewTermResponse{HeadEntryId: headEntry}},
		{MsgTypeBecomeLeaderRequest, &proto.BecomeLeaderRequest{}, MsgTypeBecomeLeaderResponse,
			&proto.BecomeLeaderResponse{}},
		{MsgTypeAddFollowerRequest, &proto.AddFollowerRequest{}, MsgTypeAddFollowerResponse,
			&proto.AddFollowerResponse{}},
		{MsgTypeTruncateRequest, &proto.TruncateRequest{}, MsgTypeTruncateResponse,
			&proto.TruncateResponse{HeadEntryId: headEntry}},
		{MsgTypeGetStatusRequest, &proto.GetStatusRequest{}, MsgTypeGetStatusResponse,
			&proto.GetStatusResponse{Term: 2, Status: proto.ServingStatus_LEADER}},
		{MsgTypeHealthCheck, &proto.GetStatusRequest{}, MsgTypeHealthCheckOk,
			wrapperspb.Int64(provider.processId)},
	}
	for i, tt := range tests {
		t.Run(string(tt.msgType), func(t *testing.T) {
			msgId := int64(100 + i)
			provider.HandleOxiaRequest(tt.msgType, oxiaRequest("n1", tt.msgType, msgId), tt.request)

			msgType, msg, response := parseLine(t, receiveLines(t, lines, 1)[0])
			require.Equal(t, tt.respType, msgType)
			res := msg.(*Message[OxiaMessage])
			require.Equal(t, "n1", res.Dest)
			require.Equal(t, msgId, *res.Body.InReplyTo)
			require.True(t, pb.Equal(tt.response, response), "response: %v", response)
		})
	}
}

func TestMaelstromGrpcProviderHandleOxiaRequestRepliesWithError(t *testing.T) {
	lines := captureStdout(t)

	// The services implement none of the methods
	provider := newMaelstromGrpcProvider()
	provider.RegisterService(&proto.OxiaCoordination_ServiceDesc, &fakeCoordinationServer{})
	provider.RegisterService(&proto.OxiaLogReplication_ServiceDesc, newRecordingReplicationServer())

	tests := []struct {
		msgType MsgType
		request pb.Message
	}{
		{MsgTypeNewTermRequest, &proto.NewTermRequest{}},
		{MsgTypeBecomeLeaderRequest, &proto.BecomeLeaderRequest{}},
		{MsgTypeAddFollowerRequest, &proto.AddFollowerRequest{}},
		{MsgTypeTruncateRequest, &proto.TruncateRequest{}},
		{MsgTypeGetStatusRequest, &proto.GetStatusRequest{}},
		{MsgTypeDeleteShardRequest, &proto.DeleteShardRequest{}},
	}
	for i, tt := range tests {
		t.Run(string(tt.msgType), func(t *testing.T) {
			msgId := int64(200 + i)
			provider.HandleOxiaRequest(tt.msgType, oxiaRequest("n1", tt.msgType, msgId), tt.request)

			msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
			require.Equal(t, MsgTypeError, msgType)
			res := msg.(*Message[ErrorResponse])
			require.Equal(t, "n1", res.Dest)
			require.Equal(t, msgId, *res.Body.InReplyTo)
			require.Equal(t, 1000, res.Body.Code)
			require.Contains(t, res.Body.Text, "not implemented")
		})
	}
}

func TestMaelstromGrpcProviderHandleOxiaRequestPanicsOnUnknownType(t *testing.T) {
	provider := newMaelstromGrpcProvider()
	require.Panics(t, func() {
		provider.HandleOxiaRequest(MsgTypeInit, oxiaRequest("n1", MsgTypeInit, 1), nil)
	})
}

func TestMaelstromGrpcProviderHandleOxiaStreamRequestIgnoresUnsupportedType(t *testing.T) {
	provider := newMaelstromGrpcProvider()
	provider.HandleOxiaStreamRequest(MsgTypeAck, appendMessage("n1", 1, 0), &proto.Ack{})
	require.Empty(t, provider.replicateStreams)
}

func TestMaelstromGrpcProviderHandleStreamErrorClosesStream(t *testing.T) {
	lines := captureStdout(t)

	follower := newRecordingReplicationServer()
	provider := newMaelstromGrpcProvider()
	provider.RegisterService(&proto.OxiaLogReplication_ServiceDesc, follower)
	provider.HandleOxiaStreamRequest(MsgTypeAppend, appendMessage("n1", 7, 0), entryAppend(0))
	require.Equal(t, int64(0), <-follower.offsets)

	// Unknown streams are ignored
	provider.HandleStreamError("n2", 7)
	provider.HandleStreamError("n1", 8)
	require.Empty(t, follower.done)

	// The leader declared the stream dead: the handler stops, and the leader
	// is told back
	provider.HandleStreamError("n1", 7)
	require.ErrorIs(t, <-follower.done, io.EOF)
	requireStreamError(t, receiveLines(t, lines, 1)[0], "n1", 7)

	// A late append doesn't resurrect the stream, and reminds the leader
	provider.HandleOxiaStreamRequest(MsgTypeAppend, appendMessage("n1", 7, 1), entryAppend(1))
	requireStreamError(t, receiveLines(t, lines, 1)[0], "n1", 7)
	require.Empty(t, follower.offsets)
}

func TestMaelstromGrpcProviderHandleClientRequestServesLinKvOperations(t *testing.T) {
	lines := captureStdout(t)

	failing := func() *fakeClientServer {
		return &fakeClientServer{
			write: func(*proto.WriteRequest) (*proto.WriteResponse, error) { return nil, errors.New("write failed") },
			read:  func(*proto.ReadRequest) (*proto.ReadResponse, error) { return nil, errors.New("read failed") },
		}
	}
	failingStatus := func() *fakeClientServer {
		return &fakeClientServer{
			write: func(*proto.WriteRequest) (*proto.WriteResponse, error) {
				return &proto.WriteResponse{Puts: []*proto.PutResponse{{Status: proto.Status_SESSION_DOES_NOT_EXIST}}}, nil
			},
			read: func(*proto.ReadRequest) (*proto.ReadResponse, error) {
				return &proto.ReadResponse{Gets: []*proto.GetResponse{{Status: proto.Status_SESSION_DOES_NOT_EXIST}}}, nil
			},
		}
	}
	kv := func() *fakeClientServer { return newKVClientServer(map[string]string{"1": "5"}) }
	// The read of a compare-and-set succeeds, and its write fails
	failingCasWrite := func(write func(*proto.WriteRequest) (*proto.WriteResponse, error)) func() *fakeClientServer {
		return func() *fakeClientServer {
			server := kv()
			server.write = write
			return server
		}
	}

	tests := []struct {
		name      string
		server    func() *fakeClientServer
		msgType   MsgType
		msg       any
		wantType  MsgType
		wantCode  int
		wantValue int64
	}{
		{"write", kv, MsgTypeWrite, writeMessage(1, 6), MsgTypeWriteOk, 0, 0},
		{"write error", failing, MsgTypeWrite, writeMessage(1, 6), MsgTypeError, 1000, 0},
		{"write failed status", failingStatus, MsgTypeWrite, writeMessage(1, 6), MsgTypeError, 1000, 0},
		{"read", kv, MsgTypeRead, readMessage(1), MsgTypeReadOk, 0, 5},
		{"read missing key", kv, MsgTypeRead, readMessage(2), MsgTypeError, 20, 0},
		{"read error", failing, MsgTypeRead, readMessage(1), MsgTypeError, 1000, 0},
		{"read failed status", failingStatus, MsgTypeRead, readMessage(1), MsgTypeError, 1000, 0},
		{"cas", kv, MsgTypeCas, casMessage(1, 5, 8), MsgTypeCasOk, 0, 0},
		{"cas precondition failed", kv, MsgTypeCas, casMessage(1, 4, 8), MsgTypeError, 22, 0},
		{"cas missing key", kv, MsgTypeCas, casMessage(2, 5, 8), MsgTypeError, 20, 0},
		{"cas read error", failing, MsgTypeCas, casMessage(1, 5, 8), MsgTypeError, 1000, 0},
		{"cas read failed status", failingStatus, MsgTypeCas, casMessage(1, 5, 8), MsgTypeError, 1000, 0},
		{"cas write error", failingCasWrite(failing().write), MsgTypeCas, casMessage(1, 5, 8), MsgTypeError, 1000, 0},
		{"cas write failed status", failingCasWrite(failingStatus().write), MsgTypeCas, casMessage(1, 5, 8),
			MsgTypeError, 1000, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := newMaelstromGrpcProvider()
			provider.RegisterService(&proto.OxiaClient_ServiceDesc, tt.server())
			require.NoError(t, provider.HandleClientRequest(tt.msgType, tt.msg))

			msgId, _, origin := getOrigin(tt.msgType, tt.msg)
			msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
			require.Equal(t, tt.wantType, msgType)
			_, inReplyTo, _ := getOrigin(msgType, msg)
			require.Equal(t, msgId, inReplyTo)
			require.Equal(t, origin, destination(msg))
			switch res := msg.(type) {
			case *Message[ErrorResponse]:
				require.Equal(t, tt.wantCode, res.Body.Code)
			case *Message[ReadResponse]:
				require.Equal(t, tt.wantValue, res.Body.Value)
			default:
				// write_ok and cas_ok carry no payload
			}
		})
	}
}

func TestMaelstromGrpcProviderHandleClientRequestAppliesWrites(t *testing.T) {
	lines := captureStdout(t)

	provider := newMaelstromGrpcProvider()
	provider.RegisterService(&proto.OxiaClient_ServiceDesc, newKVClientServer(map[string]string{}))
	require.NoError(t, provider.HandleClientRequest(MsgTypeWrite, writeMessage(1, 3)))
	receiveLines(t, lines, 1)
	require.NoError(t, provider.HandleClientRequest(MsgTypeCas, casMessage(1, 3, 4)))
	receiveLines(t, lines, 1)
	require.NoError(t, provider.HandleClientRequest(MsgTypeRead, readMessage(1)))

	msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeReadOk, msgType)
	require.Equal(t, int64(4), msg.(*Message[ReadResponse]).Body.Value)
}

func TestMaelstromGrpcProviderHandleClientRequestFailsOnUnexpectedRequest(t *testing.T) {
	err := newMaelstromGrpcProvider().HandleClientRequest(MsgTypeWriteOk, &Message[BaseMessageBody]{Src: "n3"})
	require.ErrorContains(t, err, "unexpected request write_ok")
}

func TestMaelstromReadServerStreamHeadersAreNotImplemented(t *testing.T) {
	stream := newMaelstromReadServerStream()
	require.Panics(t, func() { _ = stream.SetHeader(metadata.MD{}) })
	require.Panics(t, func() { _ = stream.SendHeader(metadata.MD{}) })
	require.Panics(t, func() { stream.SetTrailer(metadata.MD{}) })
}

func TestMaelstromReplicateServerStreamSendAcksToLeader(t *testing.T) {
	lines := captureStdout(t)

	stream := newMaelstromReplicateServerStream(appendMessage("n1", 9, 0))
	require.NoError(t, stream.Send(&proto.Ack{Offset: 4}))

	msgType, msg, ack := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeAck, msgType)
	require.Equal(t, "n1", msg.(*Message[OxiaStreamMessage]).Dest)
	require.Equal(t, int64(9), msg.(*Message[OxiaStreamMessage]).Body.StreamId)
	require.Equal(t, int64(4), ack.(*proto.Ack).Offset)

	// A reset stream acks nothing
	stream.close()
	require.Error(t, stream.Send(&proto.Ack{Offset: 5}))
}

func TestMaelstromReplicateServerStreamRecvDiscardsEntriesAfterClose(t *testing.T) {
	stream := newMaelstromReplicateServerStream(appendMessage("n1", 9, 0))
	stream.deliver(0, entryAppend(0))
	stream.close()

	_, err := stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	// Entries that arrive after the reset are dropped
	stream.deliver(1, entryAppend(1))
	require.Len(t, stream.requests, 1)
}

func TestMaelstromReplicateServerStreamDeliverSkipsDuplicates(t *testing.T) {
	stream := newMaelstromReplicateServerStream(appendMessage("n1", 9, 0))
	stream.deliver(0, entryAppend(0))
	stream.deliver(0, entryAppend(0))
	stream.deliver(1, entryAppend(1))

	for offset := range int64(2) {
		entry, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, offset, entry.Entry.Offset)
	}
	require.Empty(t, stream.requests)
}

func TestMaelstromReplicateServerStreamDeliverClosesStreamOnOverflow(t *testing.T) {
	stream := newMaelstromReplicateServerStream(appendMessage("n1", 9, 0))
	for seq := range int64(cap(stream.requests)) {
		stream.deliver(seq, entryAppend(seq))
	}
	select {
	case <-stream.done:
		require.Fail(t, "the stream closed before its buffer was full")
	default:
	}

	// The handler stopped draining the stream
	stream.deliver(int64(cap(stream.requests)), entryAppend(int64(cap(stream.requests))))
	_, err := stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestMaelstromReplicateServerStreamContextCarriesShard(t *testing.T) {
	stream := newMaelstromReplicateServerStream(appendMessage("n1", 9, 0))
	md, ok := metadata.FromIncomingContext(stream.Context())
	require.True(t, ok)
	require.Equal(t, []string{"0"}, md.Get("shard-id"))
	require.Equal(t, []string{"default"}, md.Get("namespace"))
}

func TestMaelstromReplicateServerStreamOnGapTimeoutIgnoresClosedStream(t *testing.T) {
	logs := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	stream := newMaelstromReplicateServerStream(appendMessage("n1", 9, 0))
	// The append at 0 is missing, and the one at 1 waits for it
	stream.early[1] = entryAppend(1)
	stream.close()

	stream.onGapTimeout(0)

	require.NotContains(t, logs.String(), "Replicate stream lost appends")
}

func TestMaelstromReplicateServerStreamHeadersAreNotImplemented(t *testing.T) {
	stream := newMaelstromReplicateServerStream(appendMessage("n1", 9, 0))
	require.Panics(t, func() { _ = stream.SetHeader(metadata.MD{}) })
	require.Panics(t, func() { _ = stream.SendHeader(metadata.MD{}) })
	require.Panics(t, func() { stream.SetTrailer(metadata.MD{}) })
}

func oxiaRequest(src string, msgType MsgType, msgId int64) *Message[OxiaMessage] {
	return &Message[OxiaMessage]{
		Src:  src,
		Body: OxiaMessage{BaseMessageBody: BaseMessageBody{Type: msgType, MsgId: msgId}},
	}
}

func appendMessage(src string, streamId int64, seq int64) *Message[OxiaStreamMessage] {
	return &Message[OxiaStreamMessage]{
		Src: src,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{Type: MsgTypeAppend},
			StreamId:        streamId,
			Seq:             seq,
		},
	}
}

func entryAppend(offset int64) *proto.Append {
	return &proto.Append{Term: 1, Entry: &proto.LogEntry{Term: 1, Offset: offset}}
}

func requireStreamError(t *testing.T, line string, dest string, streamId int64) {
	t.Helper()
	msgType, msg, _ := parseLine(t, line)
	require.Equal(t, MsgTypeStreamError, msgType)
	require.Equal(t, dest, msg.(*Message[OxiaStreamMessage]).Dest)
	require.Equal(t, streamId, msg.(*Message[OxiaStreamMessage]).Body.StreamId)
}

func writeMessage(key int64, value int64) *Message[Write] {
	return &Message[Write]{
		Src:  "c1",
		Body: Write{BaseMessageBody: BaseMessageBody{Type: MsgTypeWrite, MsgId: 11}, Key: key, Value: value},
	}
}

func readMessage(key int64) *Message[Read] {
	return &Message[Read]{
		Src:  "c1",
		Body: Read{BaseMessageBody: BaseMessageBody{Type: MsgTypeRead, MsgId: 12}, Key: key},
	}
}

func casMessage(key int64, from int64, to int64) *Message[Cas] {
	return &Message[Cas]{
		Src:  "c1",
		Body: Cas{BaseMessageBody: BaseMessageBody{Type: MsgTypeCas, MsgId: 13}, Key: key, From: from, To: to},
	}
}

// fakeCoordinationServer answers the coordination requests with its hooks,
// and the requests without a hook as not implemented.
type fakeCoordinationServer struct {
	proto.UnimplementedOxiaCoordinationServer

	newTerm      func(*proto.NewTermRequest) (*proto.NewTermResponse, error)
	becomeLeader func(*proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)
	addFollower  func(*proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)
	getStatus    func(*proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	deleteShard  func(*proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
}

func (f *fakeCoordinationServer) NewTerm(ctx context.Context, req *proto.NewTermRequest) (
	*proto.NewTermResponse, error) {
	if f.newTerm == nil {
		return f.UnimplementedOxiaCoordinationServer.NewTerm(ctx, req)
	}
	return f.newTerm(req)
}

func (f *fakeCoordinationServer) BecomeLeader(ctx context.Context, req *proto.BecomeLeaderRequest) (
	*proto.BecomeLeaderResponse, error) {
	if f.becomeLeader == nil {
		return f.UnimplementedOxiaCoordinationServer.BecomeLeader(ctx, req)
	}
	return f.becomeLeader(req)
}

func (f *fakeCoordinationServer) AddFollower(ctx context.Context, req *proto.AddFollowerRequest) (
	*proto.AddFollowerResponse, error) {
	if f.addFollower == nil {
		return f.UnimplementedOxiaCoordinationServer.AddFollower(ctx, req)
	}
	return f.addFollower(req)
}

func (f *fakeCoordinationServer) GetStatus(ctx context.Context, req *proto.GetStatusRequest) (
	*proto.GetStatusResponse, error) {
	if f.getStatus == nil {
		return f.UnimplementedOxiaCoordinationServer.GetStatus(ctx, req)
	}
	return f.getStatus(req)
}

func (f *fakeCoordinationServer) DeleteShard(ctx context.Context, req *proto.DeleteShardRequest) (
	*proto.DeleteShardResponse, error) {
	if f.deleteShard == nil {
		return f.UnimplementedOxiaCoordinationServer.DeleteShard(ctx, req)
	}
	return f.deleteShard(req)
}

// fakeClientServer serves the writes and the reads with its hooks.
type fakeClientServer struct {
	proto.UnimplementedOxiaClientServer

	write func(*proto.WriteRequest) (*proto.WriteResponse, error)
	read  func(*proto.ReadRequest) (*proto.ReadResponse, error)
}

func (f *fakeClientServer) Write(_ context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {
	return f.write(req)
}

func (f *fakeClientServer) Read(req *proto.ReadRequest, stream grpc.ServerStreamingServer[proto.ReadResponse]) error {
	res, err := f.read(req)
	if err != nil {
		return err
	}
	return stream.Send(res)
}

// newKVClientServer returns a client server that keeps the records in memory,
// with the versions of a conditional put.
func newKVClientServer(records map[string]string) *fakeClientServer {
	var mutex sync.Mutex
	versions := map[string]int64{}
	return &fakeClientServer{
		write: func(req *proto.WriteRequest) (*proto.WriteResponse, error) {
			mutex.Lock()
			defer mutex.Unlock()
			put := req.Puts[0]
			version := versions[put.Key]
			if put.ExpectedVersionId != nil && *put.ExpectedVersionId != version {
				return &proto.WriteResponse{Puts: []*proto.PutResponse{{Status: proto.Status_UNEXPECTED_VERSION_ID}}}, nil
			}
			records[put.Key] = string(put.Value)
			versions[put.Key] = version + 1
			return &proto.WriteResponse{Puts: []*proto.PutResponse{{Status: proto.Status_OK}}}, nil
		},
		read: func(req *proto.ReadRequest) (*proto.ReadResponse, error) {
			mutex.Lock()
			defer mutex.Unlock()
			key := req.Gets[0].Key
			value, ok := records[key]
			if !ok {
				return &proto.ReadResponse{Gets: []*proto.GetResponse{{Status: proto.Status_KEY_NOT_FOUND}}}, nil
			}
			return &proto.ReadResponse{Gets: []*proto.GetResponse{{
				Status:  proto.Status_OK,
				Value:   []byte(value),
				Version: &proto.Version{VersionId: versions[key]},
			}}}, nil
		},
	}
}

// lockedBuffer is a bytes.Buffer that can be written and read concurrently.
type lockedBuffer struct {
	sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.Lock()
	defer b.Unlock()
	return b.buffer.String()
}
