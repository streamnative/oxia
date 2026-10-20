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

package internal

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/proto"
)

// testWriteStream counts the requests sent on it. It answers each one with an
// empty response on responses, if set, and never answers them otherwise. Sending
// fails with sendErr, if set.
type testWriteStream struct {
	grpc.ClientStream
	ctx       context.Context
	sent      atomic.Int64
	sendErr   error
	responses chan *proto.WriteResponse
}

func (s *testWriteStream) Send(*proto.WriteRequest) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent.Add(1)
	if s.responses != nil {
		s.responses <- &proto.WriteResponse{}
	}
	return nil
}

func (s *testWriteStream) Recv() (*proto.WriteResponse, error) {
	select {
	case response := <-s.responses:
		return response, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *testWriteStream) Context() context.Context {
	return s.ctx
}

// A request whose deadline passed before it could be sent, e.g. while it was
// waiting for the write stream, must not be sent anymore.
func TestStreamWrapper_DoesNotSendAfterTheDeadline(t *testing.T) {
	streamCtx, cancelStream := context.WithCancel(t.Context())
	stream := &testWriteStream{ctx: streamCtx}
	sw := newStreamWrapper(0, "leader", stream, cancelStream)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	prepared := false
	_, err := sw.Send(ctx, func() (*proto.WriteRequest, error) {
		prepared = true
		return &proto.WriteRequest{}, nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, prepared)
	assert.EqualValues(t, 0, stream.sent.Load())
}

// A stream that failed to send a request is replaced for the next one.
func TestStreamWrapperSendFailsTheStream(t *testing.T) {
	errSend := errors.New("send failed")
	streamCtx, cancelStream := context.WithCancel(t.Context())
	defer cancelStream()
	stream := &testWriteStream{ctx: streamCtx, sendErr: errSend}
	sw := newStreamWrapper(0, "leader", stream, cancelStream)

	_, err := sw.Send(t.Context(), func() (*proto.WriteRequest, error) {
		return &proto.WriteRequest{}, nil
	})
	assert.ErrorIs(t, err, errSend)
	assert.True(t, sw.failed.Load())
}

func TestStreamWrapperSendReturnsResponseAndLogsItAtDebugLevel(t *testing.T) {
	logs := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	streamCtx, cancelStream := context.WithCancel(t.Context())
	defer cancelStream()
	stream := &testWriteStream{ctx: streamCtx, responses: make(chan *proto.WriteResponse, 1)}
	sw := newStreamWrapper(0, "leader", stream, cancelStream)

	response, err := sw.Send(t.Context(), func() (*proto.WriteRequest, error) {
		return &proto.WriteRequest{}, nil
	})

	require.NoError(t, err)
	assert.NotNil(t, response)
	assert.Contains(t, logs.String(), "got response")
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
