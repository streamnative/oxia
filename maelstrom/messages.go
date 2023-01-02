package main

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"
	"os"
	"oxia/proto"
)

type MsgType string

const (
	MsgTypeInit  MsgType = "init"
	MsgTypeError MsgType = "error"

	// Maelstrom "lin-kv" workload messages

	MsgTypeWrite   MsgType = "write"
	MsgTypeWriteOk MsgType = "write_ok"
	MsgTypeRead    MsgType = "read"
	MsgTypeReadOk  MsgType = "read_ok"
	MsgTypeCas     MsgType = "cas"
	MsgTypeCasOk   MsgType = "cas_ok"

	/* Oxia specific messages */

	MsgTypeFenceRequest         MsgType = "fence-req"
	MsgTypeFenceResponse        MsgType = "fence-resp"
	MsgTypeTruncateRequest      MsgType = "truncate-req"
	MsgTypeTruncateResponse     MsgType = "truncate-resp"
	MsgTypeBecomeLeaderRequest  MsgType = "leader-req"
	MsgTypeBecomeLeaderResponse MsgType = "leader-resp"
	MsgTypeAddEntryRequest      MsgType = "add-entry"
	MsgTypeAddEntryResponse     MsgType = "ack"
	MsgTypeAddFollowerRequest   MsgType = "add-follower-req"
	MsgTypeAddFollowerResponse  MsgType = "add-follower-resp"
	MsgTypeGetStatusRequest     MsgType = "get-status"
	MsgTypeGetStatusResponse    MsgType = "status"
	MsgTypeHealthCheck          MsgType = "health"
	MsgTypeHealthCheckOk        MsgType = "health-ok"

	MsgTypeShardAssignmentsResponse MsgType = "shards"
)

var (
	oxiaRequests = map[MsgType]bool{
		MsgTypeFenceRequest:        true,
		MsgTypeTruncateRequest:     true,
		MsgTypeBecomeLeaderRequest: true,
		MsgTypeAddFollowerRequest:  true,
		MsgTypeHealthCheck:         true,
		MsgTypeGetStatusRequest:    true,
	}

	oxiaResponses = map[MsgType]bool{
		MsgTypeFenceResponse:        true,
		MsgTypeTruncateResponse:     true,
		MsgTypeBecomeLeaderResponse: true,
		MsgTypeAddFollowerResponse:  true,
		MsgTypeHealthCheckOk:        true,
		MsgTypeGetStatusResponse:    true,
	}

	oxiaStreamRequests = map[MsgType]bool{
		MsgTypeAddEntryRequest:          true,
		MsgTypeAddEntryResponse:         true,
		MsgTypeShardAssignmentsResponse: true,
	}
)

func (m MsgType) isOxiaRequest() bool {
	r, ok := oxiaRequests[m]
	return ok && r
}

func (m MsgType) isOxiaStreamRequest() bool {
	r, ok := oxiaStreamRequests[m]
	return ok && r
}

func (m MsgType) isOxiaResponse() bool {
	r, ok := oxiaResponses[m]
	return ok && r
}

type Message[Body any] struct {
	Id   int64  `json:"id"`
	Src  string `json:"src"`
	Dest string `json:"dest"`
	Body Body   `json:"body"`
}

type BaseMessageBody struct {
	Type      MsgType `json:"type"`
	MsgId     int64   `json:"msg_id"`
	InReplyTo *int64  `json:"in_reply_to"`
}

type Init struct {
	BaseMessageBody
	NodeId   string   `json:"node_id"`
	NodesIds []string `json:"node_ids"`
}

type Read struct {
	BaseMessageBody
	Key int64 `json:"key"`
}

type Write struct {
	BaseMessageBody
	Key   int64 `json:"key"`
	Value int64 `json:"value"`
}

type Cas struct {
	BaseMessageBody
	Key  int64 `json:"key"`
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

type OxiaMessage struct {
	BaseMessageBody
	OxiaMsg json.RawMessage `json:"m"`
}

type OxiaStreamMessage struct {
	BaseMessageBody
	OxiaMsg  json.RawMessage `json:"m"`
	StreamId int64           `json:"stream_id"`
}

type EmptyResponse struct {
	BaseMessageBody
}

type ReadResponse struct {
	BaseMessageBody
	Value int64 `json:"value"`
}

type ErrorResponse struct {
	BaseMessageBody
	Code int    `json:"code"`
	Text string `json:"text"`
}

func sendResponse(response any) {
	b, err := json.Marshal(response)
	if err != nil {
		panic("failed to serialize json")
	}

	fmt.Fprintln(os.Stdout, string(b))
}

func sendError(msgId int64, dest string, err error) {
	sendErrorWithCode(msgId, dest, 1000, err.Error())
}

func sendErrorWithCode(msgId int64, dest string, code int, text string) {
	b, err := json.Marshal(&Message[ErrorResponse]{
		Src:  thisNode,
		Dest: dest,
		Body: ErrorResponse{
			BaseMessageBody: BaseMessageBody{
				Type:      MsgTypeError,
				MsgId:     msgIdGenerator.Add(1),
				InReplyTo: &msgId,
			},
			Code: code,
			Text: text,
		},
	})
	if err != nil {
		panic("failed to serialize json")
	}

	fmt.Fprintln(os.Stdout, string(b))
}

var jsonMsgMapping = map[MsgType]any{
	MsgTypeInit:    &Message[Init]{},
	MsgTypeWrite:   &Message[Write]{},
	MsgTypeRead:    &Message[Read]{},
	MsgTypeCas:     &Message[Cas]{},
	MsgTypeWriteOk: &Message[BaseMessageBody]{},
	MsgTypeReadOk:  &Message[ReadResponse]{},
	MsgTypeCasOk:   &Message[BaseMessageBody]{},
	MsgTypeError:   &Message[ErrorResponse]{},

	MsgTypeHealthCheck:   &Message[OxiaMessage]{},
	MsgTypeHealthCheckOk: &Message[OxiaMessage]{},

	MsgTypeAddEntryRequest: &Message[OxiaStreamMessage]{},
}

var protoMsgMapping = map[MsgType]pb.Message{
	MsgTypeFenceRequest:         &proto.FenceRequest{},
	MsgTypeFenceResponse:        &proto.FenceResponse{},
	MsgTypeTruncateRequest:      &proto.TruncateRequest{},
	MsgTypeTruncateResponse:     &proto.TruncateResponse{},
	MsgTypeBecomeLeaderRequest:  &proto.BecomeLeaderRequest{},
	MsgTypeBecomeLeaderResponse: &proto.BecomeLeaderResponse{},
	MsgTypeAddEntryRequest:      &proto.AddEntryRequest{},
	MsgTypeAddEntryResponse:     &proto.AddEntryResponse{},
	MsgTypeAddFollowerRequest:   &proto.AddFollowerRequest{},
	MsgTypeAddFollowerResponse:  &proto.AddFollowerResponse{},
	MsgTypeGetStatusRequest:     &proto.GetStatusRequest{},
	MsgTypeGetStatusResponse:    &proto.GetStatusResponse{},

	MsgTypeShardAssignmentsResponse: &proto.ShardAssignmentsResponse{},
}

func parseRequest(line string) (msgType MsgType, msg any, protoMsg pb.Message) {
	frame := &Message[BaseMessageBody]{}

	if err := json.Unmarshal([]byte(line), frame); err != nil {
		log.Fatal().Err(err).
			Str("line", line).
			Msg("failed to unmarshal")
	}

	msg = frame
	msgType = frame.Body.Type
	sm, ok := jsonMsgMapping[frame.Body.Type]
	if ok {
		// Deserialize json again with specific type
		// Deserialize again with the proper struct
		if err := json.Unmarshal([]byte(line), sm); err != nil {
			log.Fatal().Err(err).
				Msg("failed to unmarshal the proper struct")
		}

		msg = sm
	}

	protoMsg, ok = protoMsgMapping[frame.Body.Type]
	if ok {
		if msgType.isOxiaStreamRequest() {
			om := &Message[OxiaStreamMessage]{}
			if err := json.Unmarshal([]byte(line), om); err != nil {
				log.Fatal().Err(err).
					Msg("failed to unmarshal the proper struct")
			}

			msg = om
			if err := protojson.Unmarshal(om.Body.OxiaMsg, protoMsg); err != nil {
				log.Fatal().Err(err).
					Msg("failed to unmarshal proto json")
			}
		} else {
			om := &Message[OxiaMessage]{}
			if err := json.Unmarshal([]byte(line), om); err != nil {
				log.Fatal().Err(err).
					Msg("failed to unmarshal the proper struct")
			}

			msg = om
			if err := protojson.Unmarshal(om.Body.OxiaMsg, protoMsg); err != nil {
				log.Fatal().Err(err).
					Msg("failed to unmarshal proto json")
			}
		}
	}

	log.Info().
		Interface("type", msgType).
		Interface("proto-msg", protoMsg).
		Msg("received message")
	return msgType, msg, protoMsg
}
