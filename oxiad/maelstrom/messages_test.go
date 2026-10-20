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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"
)

func TestSendResponseFailsOnUnserializableMessage(t *testing.T) {
	require.ErrorContains(t, sendResponse(make(chan int)), "failed to serialize json")
}

func TestSendErrorRepliesWithGenericCode(t *testing.T) {
	setThisNode(t, "n2")
	lines := captureStdout(t)

	sendError(7, "c1", errors.New("write failed"))

	msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeError, msgType)
	res := msg.(*Message[ErrorResponse])
	require.Equal(t, "n2", res.Src)
	require.Equal(t, "c1", res.Dest)
	require.Equal(t, int64(7), *res.Body.InReplyTo)
	require.Equal(t, 1000, res.Body.Code)
	require.Equal(t, "write failed", res.Body.Text)
}

func TestParseRequestFailsOnMalformedMessage(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantErr string
	}{
		{"not json", `not json`, "failed to unmarshal"},
		{"invalid client request", `{"src":"c1","body":{"type":"write","msg_id":1,"key":"k"}}`,
			"failed to unmarshal the write message"},
		{"invalid stream message", `{"src":"n1","body":{"type":"ack","stream_id":"s","m":{}}}`,
			"failed to unmarshal the ack message"},
		{"invalid stream proto", `{"src":"n1","body":{"type":"ack","stream_id":1,"m":{"unknown":1}}}`,
			"failed to unmarshal the proto json of the ack message"},
		{"invalid proto", `{"src":"n2","body":{"type":"term-resp","in_reply_to":1,"m":{"unknown":1}}}`,
			"failed to unmarshal the proto json of the term-resp message"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := parseRequest(tt.line)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func parseLine(t *testing.T, line string) (MsgType, any, pb.Message) {
	t.Helper()
	msgType, msg, protoMsg, err := parseRequest(line)
	require.NoError(t, err)
	return msgType, msg, protoMsg
}
