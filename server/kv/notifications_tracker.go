package kv

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	pb "google.golang.org/protobuf/proto"
	"math"
	"oxia/common"
	"oxia/proto"
	"sync"
	"sync/atomic"
)

const (
	notificationsPrefix      = common.InternalKeyPrefix + "notifications"
	maxNotificationBatchSize = 100
)

var (
	lastNotificationKey = notificationKey(math.MaxInt64)
)

type notifications struct {
	batch proto.NotificationBatch
}

func newNotifications(shardId uint32, offset int64, timestamp uint64) *notifications {
	return &notifications{
		proto.NotificationBatch{
			ShardId:       shardId,
			Offset:        offset,
			Timestamp:     timestamp,
			Notifications: map[string]*proto.Notification{},
		},
	}
}

func (n *notifications) Modified(key string, version int64) {
	nType := proto.NotificationType_KeyCreated
	if version > 0 {
		nType = proto.NotificationType_KeyModified
	}
	n.batch.Notifications[key] = &proto.Notification{
		Type:    nType,
		Version: &version,
	}
}

func (n *notifications) Deleted(key string) {
	n.batch.Notifications[key] = &proto.Notification{
		Type: proto.NotificationType_KeyDeleted,
	}
}

func notificationKey(offset int64) string {
	return fmt.Sprintf("%s/%016x", notificationsPrefix, offset)
}

type notificationsTracker struct {
	sync.Mutex
	cond       common.ConditionContext
	shard      uint32
	lastOffset atomic.Int64
	closed     atomic.Bool
	kv         KV
	log        zerolog.Logger
}

func newNotificationsTracker(shard uint32, lastOffset int64, kv KV) *notificationsTracker {
	nt := &notificationsTracker{
		shard: shard,
		kv:    kv,
		log: log.Logger.With().
			Str("component", "notifications-tracker").
			Uint32("shard", shard).
			Logger(),
	}
	nt.lastOffset.Store(lastOffset)
	nt.cond = common.NewConditionContext(nt)
	return nt
}

func (nt *notificationsTracker) UpdatedCommitIndex(offset int64) {
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
		return errors.New("already closed")
	}

	return nil
}

func (nt *notificationsTracker) ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error) {
	if err := nt.waitForNotifications(ctx, startOffset); err != nil {
		return nil, err
	}

	it := nt.kv.RangeScan(notificationKey(startOffset), lastNotificationKey)
	defer it.Close()

	var res []*proto.NotificationBatch

	for count := 0; count < maxNotificationBatchSize && it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return nil, errors.Wrap(err, "failed to read notification batch")
		}

		nb := &proto.NotificationBatch{}
		if err := pb.Unmarshal(value, nb); err != nil {
			return nil, errors.Wrap(err, "failed to deserialize notification batch")
		}
		res = append(res, nb)
	}

	return res, nil
}

func (nt *notificationsTracker) Close() error {
	nt.closed.Store(true)
	nt.cond.Broadcast()
	return nil
}
