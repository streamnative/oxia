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
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
)

type Dispatcher interface {
	ReceivedMessage(msgType MsgType, m any, message pb.Message) error
	RpcRequest(ctx context.Context, dest string, msgType MsgType, message pb.Message) (pb.Message, error)
}

type ResponseError struct {
	msg pb.Message
	err error
}

// ProxiedRequest is the client request that a proxied request answers.
type ProxiedRequest struct {
	msgId  int64
	origin string
}

var msgIdGenerator atomic.Int64

type dispatcher struct {
	sync.Mutex
	requests        map[int64]chan ResponseError
	proxiedRequests map[int64]ProxiedRequest

	replicationProvider *maelstromReplicationRpcProvider
	grpcProvider        *maelstromGrpcProvider
	// Only on the coordinator node, see newRpcProvider
	coordinatorProvider *maelstromCoordinatorRpcProvider
	currentLeader       string

	// The stream and message ids of the last shard assignments applied, see
	// applyShardAssignments. Only the message loop uses them, under the
	// dispatcher lock.
	assignmentsStreamId int64
	assignmentsMsgId    int64
}

func newDispatcher(provider *maelstromGrpcProvider, replicationProvider *maelstromReplicationRpcProvider) *dispatcher {
	d := &dispatcher{
		requests:            make(map[int64]chan ResponseError),
		proxiedRequests:     make(map[int64]ProxiedRequest),
		grpcProvider:        provider,
		replicationProvider: replicationProvider,
	}

	d.replicationProvider.dispatcher = d
	return d
}

func (d *dispatcher) onOxiaRequestMessage(msgType MsgType, m *Message[OxiaMessage], message pb.Message) {
	go d.grpcProvider.HandleOxiaRequest(msgType, m, message)
}

func (d *dispatcher) onOxiaResponseMessage(_ MsgType, res *Message[OxiaMessage], message pb.Message) error {
	ch, ok := d.requests[*res.Body.InReplyTo]
	if !ok {
		return errors.Errorf("invalid response id %d", *res.Body.InReplyTo)
	}

	ch <- ResponseError{msg: message}
	return nil
}

func (d *dispatcher) onOxiaStreamRequestMessage(msgType MsgType, m any, message pb.Message) error {
	switch msgType {
	case MsgTypeShardAssignmentsResponse:
		msg := m.(*Message[OxiaStreamMessage])
		d.applyShardAssignments(msg, message.(*proto.ShardAssignments))
		// Ack every copy, also a stale one, so that the coordinator stops
		// resending it
		return sendShardAssignmentsAck(msg.Src, msg.Body.StreamId, msg.Body.MsgId)

	case MsgTypeShardAssignmentsAck:
		if d.coordinatorProvider == nil {
			return errors.Errorf("unexpected %s on a data server", msgType)
		}
		msg := m.(*Message[OxiaStreamMessage])
		d.coordinatorProvider.HandleShardAssignmentsAck(msg.Body.StreamId, *msg.Body.InReplyTo)

	case MsgTypeAppend:
		// Deliver inline, not on a per-message goroutine: goroutine scheduling
		// would reorder entries within a stream, which gRPC never does. The
		// stream delivery never blocks (buffered channel, drops on overflow).
		msg := m.(*Message[OxiaStreamMessage])
		d.grpcProvider.HandleOxiaStreamRequest(msgType, msg, message)

	case MsgTypeAck:
		streamId := m.(*Message[OxiaStreamMessage]).Body.StreamId
		d.replicationProvider.HandleAck(streamId, message.(*proto.Ack))

	case MsgTypeStreamError:
		// A peer declared one of our streams dead. The stream id alone doesn't
		// say which side of it we hold, so try both: each side ignores unknown
		// ids.
		msg := m.(*Message[OxiaStreamMessage])
		d.replicationProvider.HandleStreamError(msg.Body.StreamId)
		d.grpcProvider.HandleStreamError(msg.Src, msg.Body.StreamId)

	default:
		return errors.Errorf("unknown stream message type %s", msgType)
	}
	return nil
}

// applyShardAssignments routes the requests to the leader in the assignments.
// Each message carries all the assignments, and the coordinator gives the
// messages of a stream increasing ids: a message that is not newer than the
// last one applied from its stream, e.g. a resend that the network delayed
// past the next message, is skipped.
func (d *dispatcher) applyShardAssignments(msg *Message[OxiaStreamMessage], assignments *proto.ShardAssignments) {
	if msg.Body.StreamId == d.assignmentsStreamId && msg.Body.MsgId <= d.assignmentsMsgId {
		return
	}
	d.assignmentsStreamId = msg.Body.StreamId
	d.assignmentsMsgId = msg.Body.MsgId

	if leader, ok := getDefaultNamespaceLeader(assignments); ok {
		d.currentLeader = leader
		slog.Info(
			"Received notification of new leader",
			slog.String("leader", d.currentLeader),
		)
	}
}

func (d *dispatcher) onOxiaProxiedMessage(msgType MsgType, m any, _ pb.Message) error {
	msgId, inReplyTo, origin, err := getOrigin(msgType, m)
	if err != nil {
		return err
	}

	slog.Info(
		"Received response for proxied message",
		slog.Int64("proxied-msg-id", msgId),
		slog.Int64("in-reply-to", inReplyTo),
	)

	switch msgType {
	case MsgTypeWrite, MsgTypeRead, MsgTypeCas:
		switch d.currentLeader {
		case "":
			sendErrorWithCode(msgId, origin, 11, "temporarily-unavailable")
		case thisNode:
			// We should answer
			return d.grpcProvider.HandleClientRequest(msgType, m)
		default:
			return d.proxyToLeader(msgType, m)
		}

		// Responses for proxied requests
	case MsgTypeWriteOk, MsgTypeCasOk:
		if pr, ok := d.proxiedRequests[inReplyTo]; ok {
			delete(d.proxiedRequests, inReplyTo)
			return sendResponse(&Message[BaseMessageBody]{
				Src:  thisNode,
				Dest: pr.origin,
				Body: BaseMessageBody{
					Type:      msgType,
					InReplyTo: &pr.msgId,
				},
			})
		}

	case MsgTypeReadOk:
		if pr, ok := d.proxiedRequests[inReplyTo]; ok {
			delete(d.proxiedRequests, inReplyTo)
			return sendResponse(&Message[ReadResponse]{
				Src:  thisNode,
				Dest: pr.origin,
				Body: ReadResponse{
					BaseMessageBody: BaseMessageBody{
						Type:      MsgTypeReadOk,
						InReplyTo: &pr.msgId,
					},
					Value: m.(*Message[ReadResponse]).Body.Value,
				},
			})
		}

	case MsgTypeError:
		e := m.(*Message[ErrorResponse])
		if ch, ok := d.requests[inReplyTo]; ok {
			ch <- ResponseError{
				msg: nil,
				err: errors.New(e.Body.Text),
			}

			delete(d.requests, inReplyTo)
		} else if pr, ok := d.proxiedRequests[inReplyTo]; ok {
			// Got error for a proxied request
			delete(d.proxiedRequests, inReplyTo)
			sendErrorWithCode(pr.msgId, pr.origin, e.Body.Code, e.Body.Text)
		}
	default:
		return errors.Errorf("unknown message type %s", msgType)
	}
	return nil
}

func (d *dispatcher) ReceivedMessage(msgType MsgType, m any, message pb.Message) error {
	d.Lock()
	defer d.Unlock()

	slog.Info(
		"RECEIVED MESSAGE",
		slog.Any("msg", m),
		slog.Any("msg-type", msgType),
	)

	switch {
	case msgType.isOxiaRequest():
		d.onOxiaRequestMessage(msgType, m.(*Message[OxiaMessage]), message)
	case msgType.isOxiaResponse():
		return d.onOxiaResponseMessage(msgType, m.(*Message[OxiaMessage]), message)
	case msgType.isOxiaStreamRequest():
		return d.onOxiaStreamRequestMessage(msgType, m, message)
	default:
		return d.onOxiaProxiedMessage(msgType, m, message)
	}
	return nil
}

func getOrigin(msgType MsgType, m any) (msgId int64, inReplyTo int64, origin string, err error) {
	switch msgType {
	case MsgTypeWrite:
		w := m.(*Message[Write])
		origin = w.Src
		msgId = w.Body.MsgId
		if w.Body.InReplyTo != nil {
			inReplyTo = *w.Body.InReplyTo
		}

	case MsgTypeRead:
		r := m.(*Message[Read])
		origin = r.Src
		msgId = r.Body.MsgId
		if r.Body.InReplyTo != nil {
			inReplyTo = *r.Body.InReplyTo
		}
	case MsgTypeCas:
		c := m.(*Message[Cas])
		origin = c.Src
		msgId = c.Body.MsgId
		if c.Body.InReplyTo != nil {
			inReplyTo = *c.Body.InReplyTo
		}

	case MsgTypeWriteOk, MsgTypeCasOk:
		w := m.(*Message[BaseMessageBody])
		origin = w.Src
		msgId = w.Body.MsgId
		if w.Body.InReplyTo != nil {
			inReplyTo = *w.Body.InReplyTo
		}

	case MsgTypeReadOk:
		r := m.(*Message[ReadResponse])
		origin = r.Src
		msgId = r.Body.MsgId
		if r.Body.InReplyTo != nil {
			inReplyTo = *r.Body.InReplyTo
		}
	case MsgTypeError:
		e := m.(*Message[ErrorResponse])
		origin = e.Src
		msgId = e.Body.MsgId
		if e.Body.InReplyTo != nil {
			inReplyTo = *e.Body.InReplyTo
		}
	default:
		return 0, 0, "", errors.Errorf("unknown message type %s", msgType)
	}

	return msgId, inReplyTo, origin, nil
}

func (d *dispatcher) proxyToLeader(msgType MsgType, m any) error {
	proxyMsgId := msgIdGenerator.Add(1)

	slog.Info(
		"Added new proxy msg id",
		slog.Int64("proxied-msg-id", proxyMsgId),
		slog.Any("msg-type", msgType),
	)
	var pm any
	var pr ProxiedRequest
	switch msgType {
	case MsgTypeWrite:
		w := m.(*Message[Write])
		pr = ProxiedRequest{msgId: w.Body.MsgId, origin: w.Src}
		pm = &Message[Write]{
			Src:  thisNode,
			Dest: d.currentLeader,
			Body: Write{
				BaseMessageBody: BaseMessageBody{
					Type:  MsgTypeWrite,
					MsgId: proxyMsgId,
				},
				Key:   w.Body.Key,
				Value: w.Body.Value,
			},
		}
	case MsgTypeRead:
		r := m.(*Message[Read])
		pr = ProxiedRequest{msgId: r.Body.MsgId, origin: r.Src}
		pm = &Message[Read]{
			Src:  thisNode,
			Dest: d.currentLeader,
			Body: Read{
				BaseMessageBody: BaseMessageBody{
					Type:  MsgTypeRead,
					MsgId: proxyMsgId,
				},
				Key: r.Body.Key,
			},
		}
	case MsgTypeCas:
		cas := m.(*Message[Cas])
		pr = ProxiedRequest{msgId: cas.Body.MsgId, origin: cas.Src}
		pm = &Message[Cas]{
			Src:  thisNode,
			Dest: d.currentLeader,
			Body: Cas{
				BaseMessageBody: BaseMessageBody{
					Type:  MsgTypeCas,
					MsgId: proxyMsgId,
				},
				Key:  cas.Body.Key,
				From: cas.Body.From,
				To:   cas.Body.To,
			},
		}
	default:
		return errors.Errorf("unknown client request type %s", msgType)
	}

	d.proxiedRequests[proxyMsgId] = pr
	return sendResponse(pm)
}

func (d *dispatcher) RpcRequest(ctx context.Context, dest string, msgType MsgType, message pb.Message) (pb.Message, error) {
	msgId := msgIdGenerator.Add(1)

	oxiaMsg, err := toJSON(message)
	if err != nil {
		return nil, err
	}
	req := &Message[OxiaMessage]{
		Src:  thisNode,
		Dest: dest,
		Body: OxiaMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  msgType,
				MsgId: msgId,
			},
			OxiaMsg: oxiaMsg,
		},
	}

	b, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize json")
	}

	d.Lock()
	ch := make(chan ResponseError, 1)
	d.requests[msgId] = ch
	d.Unlock()

	slog.Info(
		"Sending message",
		slog.Any("msg", b),
	)

	fmt.Fprintln(os.Stdout, string(b))

	select {
	case w := <-ch:
		return w.msg, w.err
	case <-time.After(5 * time.Second):
		return nil, errors.New("timed out")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

var protoMarshal = protojson.MarshalOptions{
	EmitUnpopulated: true,
}

func toJSON(message pb.Message) ([]byte, error) {
	r, err := protoMarshal.Marshal(message)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize proto to json")
	}
	return r, nil
}
