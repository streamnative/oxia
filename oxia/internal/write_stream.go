package internal

import (
	"context"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"io"
	"log/slog"
	"sync"
)

type streamWrapper struct {
	sync.Mutex

	stream          proto.OxiaClient_WriteStreamClient
	pendingRequests []common.Future[*proto.WriteResponse]
}

func newStreamWrapper(stream proto.OxiaClient_WriteStreamClient) *streamWrapper {
	sw := &streamWrapper{
		stream:          stream,
		pendingRequests: nil,
	}

	go sw.handleResponses()
	go sw.handleStreamClosed()
	return sw
}

func (sw *streamWrapper) Send(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {
	f := common.NewFuture[*proto.WriteResponse]()

	sw.Lock()
	sw.pendingRequests = append(sw.pendingRequests, f)
	if err := sw.stream.Send(req); err != nil {
		sw.Unlock()
		return nil, err
	}

	sw.Unlock()

	return f.Wait(ctx)
}

func (sw *streamWrapper) handleStreamClosed() {
	select {
	case <-sw.stream.Context().Done():
		// Fail all pending requests
		sw.Lock()
		for _, f := range sw.pendingRequests {
			f.Fail(io.EOF)
		}
		sw.pendingRequests = nil
		sw.Unlock()
	}
}

func (sw *streamWrapper) handleResponses() {
	for {
		response, err := sw.stream.Recv()
		if err != nil {
			return
		}

		slog.Debug("got response",
			slog.Any("res", response),
			slog.Any("err", err),
		)

		sw.Lock()
		var f common.Future[*proto.WriteResponse]
		f, sw.pendingRequests = sw.pendingRequests[0], sw.pendingRequests[1:]
		sw.Unlock()

		f.Complete(response)
	}
}
