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
)

func TestSendResponsePanicsOnUnserializableMessage(t *testing.T) {
	require.PanicsWithValue(t, "failed to serialize json", func() {
		sendResponse(make(chan int))
	})
}

func TestSendErrorRepliesWithGenericCode(t *testing.T) {
	setThisNode(t, "n2")
	lines := captureStdout(t)

	sendError(7, "c1", errors.New("write failed"))

	msgType, msg, _ := parseRequest(receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeError, msgType)
	res := msg.(*Message[ErrorResponse])
	require.Equal(t, "n2", res.Src)
	require.Equal(t, "c1", res.Dest)
	require.Equal(t, int64(7), *res.Body.InReplyTo)
	require.Equal(t, 1000, res.Body.Code)
	require.Equal(t, "write failed", res.Body.Text)
}
