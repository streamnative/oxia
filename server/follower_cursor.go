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
	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric/unit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
	"sync/atomic"
	"time"
)

// AddEntriesStreamProvider
// This is a provider for the AddEntryStream Grpc handler
// It's used to allow passing in a mocked version of the Grpc service
type AddEntriesStreamProvider interface {
	GetAddEntriesStream(ctx context.Context, follower string, shard uint32) (proto.OxiaLogReplication_AddEntriesClient, error)
	SendSnapshot(ctx context.Context, follower string, shard uint32) (proto.OxiaLogReplication_SendSnapshotClient, error)
}

// FollowerCursor
// The FollowerCursor represents a cursor on the leader WAL that sends entries to a specific follower and receives a
// stream of acknowledgments from that follower.
type FollowerCursor interface {
	io.Closer

	ShardId() uint32

	// LastPushed
	// The last entry that was sent to this follower
	LastPushed() int64

	// AckIndex The highest entry already acknowledged by this follower
	AckIndex() int64
}

type followerCursor struct {
	sync.Mutex

	epoch                    int64
	follower                 string
	addEntriesStreamProvider AddEntriesStreamProvider
	stream                   proto.OxiaLogReplication_AddEntriesClient

	ackTracker  QuorumAckTracker
	cursorAcker CursorAcker
	wal         wal.Wal
	db          kv.DB
	lastPushed  atomic.Int64
	ackIndex    atomic.Int64
	shardId     uint32

	backoff backoff.BackOff
	closed  atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	log     zerolog.Logger

	snapshotsTransferTime     metrics.LatencyHistogram
	snapshotsStartedCounter   metrics.Counter
	snapshotsCompletedCounter metrics.Counter
	snapshotsFailedCounter    metrics.Counter
	snapshotsBytesSent        metrics.Counter
}

func NewFollowerCursor(
	follower string,
	epoch int64,
	shardId uint32,
	addEntriesStreamProvider AddEntriesStreamProvider,
	ackTracker QuorumAckTracker,
	wal wal.Wal,
	db kv.DB,
	ackIndex int64) (FollowerCursor, error) {

	labels := map[string]any{
		"shard":    shardId,
		"follower": follower,
	}

	fc := &followerCursor{
		epoch:                    epoch,
		follower:                 follower,
		ackTracker:               ackTracker,
		addEntriesStreamProvider: addEntriesStreamProvider,
		wal:                      wal,
		db:                       db,
		shardId:                  shardId,

		log: log.With().
			Str("component", "follower-cursor").
			Uint32("shard", shardId).
			Int64("epoch", epoch).
			Str("follower", follower).
			Logger(),

		snapshotsTransferTime: metrics.NewLatencyHistogram("oxia_server_snapshots_transfer_time",
			"The time taken to transfer a full snapshot", labels),
		snapshotsStartedCounter: metrics.NewCounter("oxia_server_snapshots_started",
			"The number of DB snapshots started", "count", labels),
		snapshotsCompletedCounter: metrics.NewCounter("oxia_server_snapshots_completed",
			"The number of DB snapshots completed", "count", labels),
		snapshotsFailedCounter: metrics.NewCounter("oxia_server_snapshots_failed",
			"The number of DB snapshots failed", "count", labels),
		snapshotsBytesSent: metrics.NewCounter("oxia_server_snapshots_sent",
			"The amount of data sent as snapshot", unit.Bytes, labels),
	}

	fc.ctx, fc.cancel = context.WithCancel(context.Background())
	fc.backoff = common.NewBackOff(fc.ctx)

	fc.lastPushed.Store(ackIndex)
	fc.ackIndex.Store(ackIndex)

	var err error
	if fc.cursorAcker, err = ackTracker.NewCursorAcker(ackIndex); err != nil {
		return nil, err
	}

	go common.DoWithLabels(map[string]string{
		"oxia":  "follower-cursor-send",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() {
		fc.run()
	})

	return fc, nil
}

func (fc *followerCursor) shouldSendSnapshot() bool {
	fc.Lock()
	defer fc.Unlock()

	ackIndex := fc.ackIndex.Load()
	walFirstOffset := fc.wal.FirstOffset()

	if ackIndex == wal.InvalidOffset && fc.ackTracker.CommitIndex() >= 0 {
		fc.log.Info().
			Int64("follower-ack-index", ackIndex).
			Int64("leader-commit-index", fc.ackTracker.CommitIndex()).
			Msg("Sending snapshot to empty follower")
		return true
	} else if walFirstOffset > 0 && ackIndex < walFirstOffset {
		fc.log.Info().
			Int64("follower-ack-index", ackIndex).
			Int64("wal-first-offset", fc.wal.FirstOffset()).
			Int64("wal-last-offset", fc.wal.LastOffset()).
			Msg("The follower is behind the first available entry in the leader WAL")
		return true
	}

	// No snapshot, just tail the log
	return false
}

func (fc *followerCursor) Close() error {
	fc.closed.Store(true)
	fc.cancel()

	fc.Lock()
	defer fc.Unlock()

	if fc.stream != nil {
		return fc.stream.CloseSend()
	}

	return nil
}

func (fc *followerCursor) ShardId() uint32 {
	return fc.shardId
}

func (fc *followerCursor) LastPushed() int64 {
	return fc.lastPushed.Load()
}

func (fc *followerCursor) AckIndex() int64 {
	return fc.ackIndex.Load()
}

func (fc *followerCursor) run() {
	_ = backoff.RetryNotify(fc.runOnce, fc.backoff,
		func(err error, duration time.Duration) {
			fc.log.Error().Err(err).
				Dur("retry-after", duration).
				Msg("Error while pushing entries to follower")
		})
}

func (fc *followerCursor) runOnce() error {
	if fc.shouldSendSnapshot() {
		timer := fc.snapshotsTransferTime.Timer()

		if err := fc.sendSnapshot(); err != nil {
			fc.snapshotsFailedCounter.Inc()
			return err
		}

		timer.Done()
	}

	return fc.streamEntries()
}

func (fc *followerCursor) sendSnapshot() error {
	fc.Lock()
	defer fc.Unlock()

	fc.snapshotsStartedCounter.Inc()

	ctx, cancel := context.WithCancel(fc.ctx)
	defer cancel()

	stream, err := fc.addEntriesStreamProvider.SendSnapshot(ctx, fc.follower, fc.shardId)
	if err != nil {
		return err
	}

	snapshot, err := fc.db.Snapshot()
	if err != nil {
		return err
	}

	defer snapshot.Close()

	var chunksCount, totalSize int64
	startTime := time.Now()

	for ; snapshot.Valid(); snapshot.Next() {
		chunk := snapshot.Chunk()
		content, err := chunk.Content()
		if err != nil {
			return err
		}

		fc.log.Debug().
			Str("chunk-name", chunk.Name()).
			Int("chunk-size", len(content)).
			Msg("Sending snapshot chunk")

		if err := stream.Send(&proto.SnapshotChunk{
			Epoch:   fc.epoch,
			Name:    chunk.Name(),
			Content: content,
		}); err != nil {
			return err
		}

		chunksCount++
		size := len(content)
		totalSize += int64(size)
		fc.snapshotsBytesSent.Add(size)
	}

	fc.log.Debug().
		Msg("Sent the complete snapshot, waiting for response")

	// Sent all the chunks. Wait for the follower ack
	response, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	elapsedTime := time.Since(startTime)
	throughput := float64(totalSize) / elapsedTime.Seconds()

	fc.log.Info().
		Int64("chunks-count", chunksCount).
		Str("total-size", humanize.IBytes(uint64(totalSize))).
		Stringer("elapsed-time", elapsedTime).
		Str("throughput", fmt.Sprintf("%s/s", humanize.IBytes(uint64(throughput)))).
		Int64("follower-ack-index", response.AckIndex).
		Msg("Successfully sent snapshot to follower")
	fc.ackIndex.Store(response.AckIndex)
	fc.snapshotsCompletedCounter.Inc()
	return nil
}

func (fc *followerCursor) streamEntries() error {
	ctx, cancel := context.WithCancel(fc.ctx)
	defer cancel()

	fc.Lock()
	var err error
	if fc.stream, err = fc.addEntriesStreamProvider.GetAddEntriesStream(ctx, fc.follower, fc.shardId); err != nil {
		fc.Unlock()
		return err
	}
	fc.Unlock()

	currentOffset := fc.ackIndex.Load()

	reader, err := fc.wal.NewReader(currentOffset)
	if err != nil {
		return err
	}
	defer reader.Close()

	go common.DoWithLabels(map[string]string{
		"oxia":  "follower-cursor-receive",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() {
		fc.receiveAcks(cancel, fc.stream)
	})

	fc.log.Info().
		Int64("ack-index", currentOffset).
		Msg("Successfully attached cursor follower")

	for {
		if fc.closed.Load() {
			return nil
		}

		if !reader.HasNext() {
			// We have reached the head of the wal
			// Wait for more entries to be written
			fc.ackTracker.WaitForHeadIndex(currentOffset + 1)

			continue
		}

		le, err := reader.ReadNext()
		if err != nil {
			return err
		}

		fc.log.Debug().
			Int64("offset", le.Offset).
			Msg("Sending entries to follower")

		if err = fc.stream.Send(&proto.AddEntryRequest{
			Epoch:       fc.epoch,
			Entry:       le,
			CommitIndex: fc.ackTracker.CommitIndex(),
		}); err != nil {
			return err
		}

		fc.lastPushed.Store(le.Offset)
		currentOffset = le.Offset

		// Since we've made progress, we can reset the backoff to initial setting
		fc.backoff.Reset()
	}
}

func (fc *followerCursor) receiveAcks(cancel context.CancelFunc, stream proto.OxiaLogReplication_AddEntriesClient) {
	for {
		res, err := stream.Recv()
		if err != nil {
			if status.Code(err) != codes.Canceled && status.Code(err) != codes.Unavailable {
				fc.log.Warn().Err(err).
					Msg("Error while receiving acks")
			}

			cancel()
			return
		}

		if res == nil {
			// Stream was closed by server side
			return
		}

		fc.log.Debug().
			Int64("offset", res.Offset).
			Msg("Received ack")
		fc.cursorAcker.Ack(res.Offset)

		fc.ackIndex.Store(res.Offset)
	}
}
