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

package wal

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"time"
)

const (
	DefaultRetention     = 1 * time.Hour
	DefaultCheckInterval = 10 * time.Minute
)

type CommitOffsetProvider interface {
	CommitOffset() int64
}

type Trimmer interface {
	io.Closer
}

func NewTrimmer(namespace string, shard uint32, wal Wal, retention time.Duration, checkInterval time.Duration, clock common.Clock,
	commitOffsetProvider CommitOffsetProvider) Trimmer {
	if retention.Nanoseconds() == 0 {
		retention = DefaultRetention
	}

	t := &trimmer{
		wal:                  wal,
		retention:            retention,
		clock:                clock,
		ticker:               time.NewTicker(checkInterval),
		commitOffsetProvider: commitOffsetProvider,
		waitClose:            make(chan any),
		log: log.With().
			Str("component", "wal-trimmer").
			Str("namespace", namespace).
			Uint32("shard", shard).
			Logger(),
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())

	go common.DoWithLabels(map[string]string{
		"oxia":  "wal-trimmer",
		"shard": fmt.Sprintf("%d", shard),
	}, t.run)

	return t
}

type trimmer struct {
	wal                  Wal
	retention            time.Duration
	clock                common.Clock
	ticker               *time.Ticker
	commitOffsetProvider CommitOffsetProvider
	ctx                  context.Context
	cancel               context.CancelFunc
	log                  zerolog.Logger

	waitClose chan any
}

func (t *trimmer) Close() error {
	t.cancel()
	t.ticker.Stop()

	<-t.waitClose
	return nil
}

func (t *trimmer) run() {
	for {
		select {
		case <-t.ticker.C:
			if err := t.doTrim(); err != nil {
				t.log.Error().Err(err).
					Msg("Failed to trim the wal")
			}

		case <-t.ctx.Done():
			close(t.waitClose)
			return
		}
	}
}

func (t *trimmer) doTrim() error {
	t.log.Debug().
		Int64("first-offset", t.wal.FirstOffset()).
		Int64("last-offset", t.wal.LastOffset()).
		Msg("Starting wal trimming")

	if t.wal.LastOffset() == InvalidOffset {
		return nil
	}

	cutoffTime := t.clock.Now().Add(-t.retention)

	// Check if first entry has expired
	tsFirst, err := t.readAtOffset(t.wal.FirstOffset())
	if err != nil {
		return err
	}

	t.log.Debug().
		Time("timestamp-first-entry", tsFirst).
		Time("cutoff-time", cutoffTime).
		Msg("Starting wal trimming")

	if cutoffTime.Before(tsFirst) {
		// First entry has not expired. We don't need to check more
		return nil
	}

	trimOffset, err := t.binarySearch(t.wal.FirstOffset(), t.wal.LastOffset(), cutoffTime)
	if err != nil {
		return errors.Wrap(err, "failed to perform binary search")
	}

	// We cannot trim past the commit offset, or we won't be able to replicate those entries
	commitOffset := t.commitOffsetProvider.CommitOffset()
	if commitOffset < trimOffset {
		trimOffset = commitOffset
	}

	err = t.wal.Trim(trimOffset)
	if err != nil {
		return errors.Wrap(err, "failed to trim wal")
	}

	t.log.Debug().
		Int64("trimmed-offset", trimOffset).
		Int64("first-offset", t.wal.FirstOffset()).
		Int64("last-offset", t.wal.LastOffset()).
		Msg("Successfully trimmed the wal")
	return nil
}

// Perform binary search to find the highest entry that falls within the cutoff time
func (t *trimmer) binarySearch(firstOffset, lastOffset int64, cutoffTime time.Time) (int64, error) {
	for firstOffset < lastOffset {
		med := (firstOffset + lastOffset) / 2
		// Take the ceiling
		if (firstOffset+lastOffset)%2 > 0 {
			med++
		}
		tsMed, err := t.readAtOffset(med)
		if err != nil {
			return InvalidOffset, err
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

func (t *trimmer) readAtOffset(offset int64) (timestamp time.Time, err error) {
	reader, err := t.wal.NewReader(offset - 1)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "failed to create reader")
	}

	fe, err := reader.ReadNext()
	if err != nil {
		return time.Time{}, errors.Wrap(err, "failed to read from wal")
	}

	return time.UnixMilli(int64(fe.Timestamp)), nil
}
