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
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	pb "google.golang.org/protobuf/proto"
	"oxia/common"
	"oxia/proto"
	"time"
)

const (
	minNotificationTrimmingInterval = 500 * time.Millisecond
	maxNotificationTrimmingInterval = 5 * time.Minute
)

type notificationsTrimmer struct {
	ctx                        context.Context
	waitClose                  common.WaitGroup
	kv                         KV
	interval                   time.Duration
	notificationsRetentionTime time.Duration
	clock                      common.Clock
	log                        zerolog.Logger
}

func newNotificationsTrimmer(ctx context.Context, shardId uint32, kv KV, notificationRetentionTime time.Duration, waitClose common.WaitGroup, clock common.Clock) *notificationsTrimmer {
	interval := notificationRetentionTime / 10
	if interval < minNotificationTrimmingInterval {
		interval = minNotificationTrimmingInterval
	}
	if interval > maxNotificationTrimmingInterval {
		interval = maxNotificationTrimmingInterval
	}

	t := &notificationsTrimmer{
		ctx:                        ctx,
		waitClose:                  waitClose,
		kv:                         kv,
		interval:                   interval,
		notificationsRetentionTime: notificationRetentionTime,
		clock:                      clock,
		log: log.With().
			Str("component", "db-notifications-trimmer").
			Uint32("shard", shardId).
			Logger(),
	}

	go common.DoWithLabels(map[string]string{
		"oxia":  "notifications-trimmer",
		"shard": fmt.Sprintf("%d", shardId),
	}, t.run)

	return t
}

func (t *notificationsTrimmer) run() {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := t.trimNotifications(); err != nil {
				t.log.Warn().Err(err).
					Msg("Failed to trim notifications")
			}

		case <-t.ctx.Done():
			t.waitClose.Done()
			return
		}
	}
}

func (t *notificationsTrimmer) trimNotifications() error {
	first, last, err := t.getFirstLast()
	if err != nil {
		return err
	}

	t.log.Debug().
		Int64("first-offset", first).
		Int64("last-offset", last).
		Time("current-time", t.clock.Now()).
		Dur("retention-time", t.notificationsRetentionTime).
		Msg("Starting notifications trimming")

	if last == -1 {
		return nil
	}

	cutoffTime := t.clock.Now().Add(-t.notificationsRetentionTime)

	// Check if first entry has expired
	tsFirst, err := t.readAt(first)
	if err != nil {
		return err
	}

	t.log.Debug().
		Time("timestamp-first-entry", tsFirst).
		Time("cutoff-time", cutoffTime).
		Msg("Starting notifications trimming")

	if cutoffTime.Before(tsFirst) {
		// First entry has not expired. We don't need to check more
		return nil
	}

	trimOffset, err := t.binarySearch(first, last, cutoffTime)
	if err != nil {
		return errors.Wrap(err, "failed to perform binary search")
	}

	wb := t.kv.NewWriteBatch()
	if err = wb.DeleteRange(notificationKey(first), notificationKey(trimOffset+1)); err != nil {
		return err
	}

	if err = wb.Commit(); err != nil {
		return err
	}

	if err = wb.Close(); err != nil {
		return err
	}

	t.log.Debug().
		Int64("trimmed-offset", trimOffset).
		Int64("first-offset", first).
		Int64("last-offset", last).
		Msg("Successfully trimmed the notification")
	return nil
}

func (t *notificationsTrimmer) getFirstLast() (first, last int64, err error) {
	it1 := t.kv.KeyRangeScan(firstNotificationKey, lastNotificationKey)
	defer it1.Close()
	if !it1.Valid() {
		// There are no entries in DB
		return -1, -1, nil
	}

	if first, err = parseNotificationKey(it1.Key()); err != nil {
		return first, last, err
	}

	it2 := t.kv.KeyRangeScanReverse(firstNotificationKey, lastNotificationKey)
	defer it2.Close()
	if !it2.Valid() {
		// There are no entries in DB
		return -1, -1, nil
	}

	if last, err = parseNotificationKey(it2.Key()); err != nil {
		return first, last, err
	}

	return first, last, nil
}

// Perform binary search to find the highest entry that falls within the cutoff time
func (t *notificationsTrimmer) binarySearch(firstOffset, lastOffset int64, cutoffTime time.Time) (int64, error) {
	for firstOffset < lastOffset {
		med := (firstOffset + lastOffset) / 2
		// Take the ceiling
		if (firstOffset+lastOffset)%2 > 0 {
			med++
		}
		tsMed, err := t.readAt(med)
		if err != nil {
			return -1, err
		}

		if cutoffTime.Before(tsMed) {
			// The entry at position `med` has not expired yet
			lastOffset = med - 1
		} else {
			// The entry has expired
			firstOffset = med
		}
	}

	return firstOffset, nil
}

func (t *notificationsTrimmer) readAt(offset int64) (time.Time, error) {
	res, closer, err := t.kv.Get(notificationKey(offset))
	if err != nil {
		return time.Time{}, err
	}

	defer closer.Close()

	nb := &proto.NotificationBatch{}
	if err := pb.Unmarshal(res, nb); err != nil {
		return time.Time{}, errors.Wrap(err, "failed to deserialize notification batch")
	}

	return time.UnixMilli(int64(nb.Timestamp)), nil
}
