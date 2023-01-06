package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/metadata"
	"os"
	"oxia/proto"
	"sync"
)

type maelstromReplicationRpcProvider struct {
	sync.Mutex

	dispatcher Dispatcher

	addEntryStreams map[int64]*maelstromAddEntriesClient
}

func newMaelstromReplicationRpcProvider() *maelstromReplicationRpcProvider {
	return &maelstromReplicationRpcProvider{
		addEntryStreams: make(map[int64]*maelstromAddEntriesClient),
	}
}

func (r *maelstromReplicationRpcProvider) Close() error {
	return nil
}

func (r *maelstromReplicationRpcProvider) GetAddEntriesStream(ctx context.Context, follower string, shard uint32) (
	proto.OxiaLogReplication_AddEntriesClient, error) {
	s := &maelstromAddEntriesClient{
		ctx:       ctx,
		follower:  follower,
		shard:     shard,
		streamId:  msgIdGenerator.Add(1),
		md:        make(metadata.MD),
		responses: make(chan *proto.AddEntryResponse),
	}

	r.Lock()
	defer r.Unlock()
	r.addEntryStreams[s.streamId] = s
	return s, nil
}

func (r *maelstromReplicationRpcProvider) HandleAddEntryResponse(streamId int64, res *proto.AddEntryResponse) {
	s, ok := r.addEntryStreams[streamId]
	if !ok {
		log.Warn().Int64("stream-id", streamId).Msg("Stream not found")
		return
	}

	s.responses <- res
}

func (r *maelstromReplicationRpcProvider) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	if res, err := r.dispatcher.RpcRequest(context.Background(), follower, MsgTypeTruncateRequest, req); err != nil {
		return nil, err
	} else {
		return res.(*proto.TruncateResponse), nil
	}
}

func (r *maelstromReplicationRpcProvider) SendSnapshot(ctx context.Context, follower string, shard uint32) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	panic("not implemented")
}

// //////// AddEntriesClient
type maelstromAddEntriesClient struct {
	BaseStream

	ctx       context.Context
	follower  string
	shard     uint32
	streamId  int64
	md        metadata.MD
	responses chan *proto.AddEntryResponse
}

func (m *maelstromAddEntriesClient) Send(request *proto.AddEntryRequest) error {
	b, err := json.Marshal(&Message[OxiaStreamMessage]{
		Src:  thisNode,
		Dest: m.follower,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  MsgTypeAddEntryRequest,
				MsgId: msgIdGenerator.Add(1),
			},
			OxiaMsg:  toJson(request),
			StreamId: m.streamId,
		},
	})
	if err != nil {
		panic("failed to serialize json")
	}

	fmt.Fprintln(os.Stdout, string(b))
	return nil
}

func (m *maelstromAddEntriesClient) Recv() (*proto.AddEntryResponse, error) {
	select {
	case r := <-m.responses:
		return r, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *maelstromAddEntriesClient) Header() (metadata.MD, error) {
	return m.md, nil
}
