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

package kv

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
)

const (
	notificationsPrefix      = common.InternalKeyPrefix + "notifications"
	maxNotificationBatchSize = 100
)

var (
	firstNotificationKey = notificationKey(0)
	lastNotificationKey  = notificationKey(math.MaxInt64)

	notificationsPrefixScanFormat = fmt.Sprintf("%s/%%016x", notificationsPrefix)
)

type notifications struct {
	batch proto.NotificationBatch
}

func newNotifications(shardId int64, offset int64, timestamp uint64) *notifications {
	return &notifications{
		proto.NotificationBatch{
			ShardId:       shardId,
			Offset:        offset,
			Timestamp:     timestamp,
			Notifications: map[string]*proto.Notification{},
		},
	}
}

func (n *notifications) Modified(key string, versionId, modificationsCount int64) {
	nType := proto.NotificationType_KEY_CREATED
	if modificationsCount > 0 {
		nType = proto.NotificationType_KEY_MODIFIED
	}
	n.batch.Notifications[key] = &proto.Notification{
		Type:      nType,
		VersionId: &versionId,
	}
}

func (n *notifications) Deleted(key string) {
	n.batch.Notifications[key] = &proto.Notification{
		Type: proto.NotificationType_KEY_DELETED,
	}
}

func notificationKey(offset int64) string {
	return fmt.Sprintf("%s/%016x", notificationsPrefix, offset)
}

func parseNotificationKey(key string) (offset int64, err error) {
	if _, err = fmt.Sscanf(key, notificationsPrefixScanFormat, &offset); err != nil {
		return offset, err
	}
	return offset, nil
}

type notificationsTracker struct {
	sync.Mutex
	cond       common.ConditionContext
	shard      int64
	lastOffset atomic.Int64
	closed     atomic.Bool
	kv         KV
	log        zerolog.Logger

	ctx       context.Context
	cancel    context.CancelFunc
	waitClose common.WaitGroup

	readCounter      metrics.Counter
	readBatchCounter metrics.Counter
	readBytesCounter metrics.Counter
}

func newNotificationsTracker(namespace string, shard int64, lastOffset int64, kv KV, notificationRetentionTime time.Duration, clock common.Clock) *notificationsTracker {
	labels := metrics.LabelsForShard(namespace, shard)
	nt := &notificationsTracker{
		shard:     shard,
		kv:        kv,
		waitClose: common.NewWaitGroup(1),
		log: log.Logger.With().
			Str("component", "notifications-tracker").
			Str("namespace", namespace).
			Int64("shard", shard).
			Logger(),
		readCounter: metrics.NewCounter("oxia_server_notifications_read",
			"The total number of notifications", "count", labels),
		readBatchCounter: metrics.NewCounter("oxia_server_notifications_read_batches",
			"The total number of notification batches", "count", labels),
		readBytesCounter: metrics.NewCounter("oxia_server_notifications_read",
			"The total size in bytes of notifications reads", metrics.Bytes, labels),
	}
	nt.lastOffset.Store(lastOffset)
	nt.cond = common.NewConditionContext(nt)
	nt.ctx, nt.cancel = context.WithCancel(context.Background())
	newNotificationsTrimmer(nt.ctx, namespace, shard, kv, notificationRetentionTime, nt.waitClose, clock)
	return nt
}

func (nt *notificationsTracker) UpdatedCommitOffset(offset int64) {
	nt.lastOffset.Store(offset)
	nt.cond.Broadcast()
}

func (nt *notificationsTracker) waitForNotifications(ctx context.Context, startOffset int64) error {
	nt.Lock()
	defer nt.Unlock()

	for startOffset > nt.lastOffset.Load() && !nt.closed.Load() {
		nt.log.Debug().
			Int64("start-offset", startOffset).
			Int64("last-committed-offset", nt.lastOffset.Load()).
			Msg("Waiting for notification to be available")
		if err := nt.cond.Wait(ctx); err != nil {
			return err
		}
	}

	if nt.closed.Load() {
		return common.ErrorAlreadyClosed
	}

	return nil
}

func (nt *notificationsTracker) ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error) {
	if err := nt.waitForNotifications(ctx, startOffset); err != nil {
		return nil, err
	}

	it, err := nt.kv.RangeScan(notificationKey(startOffset), lastNotificationKey)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var res []*proto.NotificationBatch

	totalCount := 0
	totalSize := 0

	for count := 0; count < maxNotificationBatchSize && it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return nil, errors.Wrap(err, "failed to read notification batch")
		}

		nb := &proto.NotificationBatch{}
		if err := nb.UnmarshalVT(value); err != nil {
			return nil, errors.Wrap(err, "failed to deserialize notification batch")
		}
		res = append(res, nb)

		totalSize += len(value)
		totalCount += len(nb.Notifications)
	}

	nt.readBatchCounter.Add(len(res))
	nt.readBytesCounter.Add(totalSize)
	nt.readCounter.Add(totalCount)
	return res, nil
}

func (nt *notificationsTracker) Close() error {
	nt.cancel()
	nt.closed.Store(true)
	nt.cond.Broadcast()
	return nt.waitClose.Wait(context.Background())
}
