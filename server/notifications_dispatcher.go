// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/streamnative/oxia/common/process"
	"github.com/streamnative/oxia/common/rpc"

	"github.com/streamnative/oxia/proto"
)

type notificationDispatcher struct {
	lc     *leaderController
	id     int64
	req    *proto.NotificationsRequest
	stream proto.OxiaClient_GetNotificationsServer

	ctx    context.Context
	cancel context.CancelFunc

	closeCh chan any
	log     *slog.Logger
}

var notificationDispatcherIdGen atomic.Int64

func startNotificationDispatcher(lc *leaderController, req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	nd := &notificationDispatcher{
		lc:      lc,
		id:      notificationDispatcherIdGen.Add(1),
		req:     req,
		stream:  stream,
		log:     lc.log.With(slog.String("component", "notification-dispatcher")),
		closeCh: make(chan any),
	}

	lc.Lock()
	lc.notificationDispatchers[nd.id] = nd
	lc.Unlock()

	// Create a context for handling this stream
	nd.ctx, nd.cancel = context.WithCancel(stream.Context())

	go process.DoWithLabels(
		nd.ctx,
		map[string]string{
			"oxia":  "dispatch-notifications",
			"shard": fmt.Sprintf("%d", lc.shardId),
			"peer":  rpc.GetPeer(stream.Context()),
		},
		func() {
			if err := nd.dispatchNotifications(); err != nil && !errors.Is(err, context.Canceled) {
				nd.log.Warn(
					"Failed to dispatch notifications",
					slog.Any("error", err),
					slog.String("peer", rpc.GetPeer(stream.Context())),
				)
				nd.cancel()
			}

			close(nd.closeCh)

			// Clean up dispatcher for leader controller map
			nd.lc.Lock()
			delete(nd.lc.notificationDispatchers, nd.id)
			nd.lc.Unlock()
		},
	)

	select {
	case <-lc.ctx.Done():
		// Leader is getting closed
		nd.cancel()
		return lc.ctx.Err()

	case <-nd.ctx.Done():
		return nd.ctx.Err()

	case <-stream.Context().Done():
		// The stream is getting closed
		nd.cancel()
		return stream.Context().Err()
	}
}

func (nd *notificationDispatcher) dispatchNotifications() error {
	nd.log.Debug(
		"Dispatch notifications",
		slog.Any("start-offset-exclusive", nd.req.StartOffsetExclusive),
	)

	var offsetInclusive int64
	if nd.req.StartOffsetExclusive != nil {
		offsetInclusive = *nd.req.StartOffsetExclusive + 1
	} else {
		nd.lc.Lock()
		qat := nd.lc.quorumAckTracker
		nd.lc.Unlock()

		if qat == nil {
			return errors.New("leader is not yet ready")
		}
		commitOffset := qat.CommitOffset()

		// The client is creating a new notification stream and wants to receive the notification from the next
		// entry that will be written.
		// In order to ensure the client will positioned on a given offset, we need to send a first "dummy"
		// notification. The client will wait for this first notification before making the notification
		// channel available to the application
		nd.log.Debug(
			"Sending first dummy notification",
			slog.Int64("commit-offset", commitOffset),
		)
		if err := nd.stream.Send(&proto.NotificationBatch{
			Shard:         nd.lc.shardId,
			Offset:        commitOffset,
			Timestamp:     0,
			Notifications: nil,
		}); err != nil {
			return err
		}

		offsetInclusive = commitOffset + 1
	}

	return nd.iterateOverNotifications(offsetInclusive)
}

func (nd *notificationDispatcher) iterateOverNotifications(startOffsetInclusive int64) error {
	lc := nd.lc
	offsetInclusive := startOffsetInclusive
	for nd.ctx.Err() == nil {
		notifications, err := lc.db.ReadNextNotifications(nd.ctx, offsetInclusive)
		if err != nil {
			return err
		}

		nd.log.Debug(
			"Got a new list of notification batches",
			slog.Int("list-size", len(notifications)),
		)

		for _, n := range notifications {
			if err := nd.stream.Send(n); err != nil {
				return err
			}
		}

		offsetInclusive += int64(len(notifications))
	}

	return nd.ctx.Err()
}

func (nd *notificationDispatcher) close() {
	// Wait for dispatcher stream to be fully closed
	<-nd.closeCh
}
