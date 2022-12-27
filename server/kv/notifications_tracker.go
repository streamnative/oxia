package kv

import (
	"fmt"
	"github.com/pkg/errors"
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
	cond       *sync.Cond
	lastOffset atomic.Int64
	closed     atomic.Bool
	kv         KV
}

func newNotificationsTracker(lastOffset int64, kv KV) *notificationsTracker {
	nt := &notificationsTracker{
		kv: kv,
	}
	nt.lastOffset.Store(lastOffset)
	nt.cond = sync.NewCond(nt)
	return nt
}

func (nt *notificationsTracker) UpdatedCommitIndex(offset int64) {
	nt.lastOffset.Store(offset)
	nt.cond.Broadcast()
}

func (nt *notificationsTracker) ReadNextNotifications(startOffset int64) ([]*proto.NotificationBatch, error) {
	nt.Lock()
	for startOffset > nt.lastOffset.Load() && !nt.closed.Load() {
		nt.cond.Wait()
	}
	nt.Unlock()

	if nt.closed.Load() {
		return nil, errors.New("already closed")
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
	return nil
}
