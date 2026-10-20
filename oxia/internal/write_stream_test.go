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
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/proto"
)

// silentWriteStream counts the requests sent on it, and never answers them.
// Sending fails with sendErr, if set.
type silentWriteStream struct {
	grpc.ClientStream
	ctx     context.Context
	sent    atomic.Int64
	sendErr error
}

func (s *silentWriteStream) Send(*proto.WriteRequest) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent.Add(1)
	return nil
}

func (s *silentWriteStream) Recv() (*proto.WriteResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *silentWriteStream) Context() context.Context {
	return s.ctx
}

// A request whose deadline passed before it could be sent, e.g. while it was
// waiting for the write stream, must not be sent anymore.
func TestStreamWrapper_DoesNotSendAfterTheDeadline(t *testing.T) {
	streamCtx, cancelStream := context.WithCancel(t.Context())
	stream := &silentWriteStream{ctx: streamCtx}
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
	stream := &silentWriteStream{ctx: streamCtx, sendErr: errSend}
	sw := newStreamWrapper(0, "leader", stream, cancelStream)

	_, err := sw.Send(t.Context(), func() (*proto.WriteRequest, error) {
		return &proto.WriteRequest{}, nil
	})
	assert.ErrorIs(t, err, errSend)
	assert.True(t, sw.failed.Load())
}
