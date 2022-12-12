package server

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"oxia/common"
	"oxia/proto"
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
}

// FollowerCursor
// The FollowerCursor represents a cursor on the leader WAL that sends entries to a specific follower and receives a
// stream of acknowledgments from that follower.
type FollowerCursor interface {
	io.Closer

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
	lastPushed  atomic.Int64
	ackIndex    atomic.Int64
	shardId     uint32

	backoff backoff.BackOff
	closed  atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	log     zerolog.Logger
}

func NewFollowerCursor(
	follower string,
	epoch int64,
	shardId uint32,
	addEntriesStreamProvider AddEntriesStreamProvider,
	ackTracker QuorumAckTracker,
	wal wal.Wal,
	ackIndex int64) (FollowerCursor, error) {

	fc := &followerCursor{
		epoch:                    epoch,
		follower:                 follower,
		ackTracker:               ackTracker,
		addEntriesStreamProvider: addEntriesStreamProvider,
		wal:                      wal,

		log: log.With().
			Str("component", "follower-cursor").
			Uint32("shard", shardId).
			Int64("epoch", epoch).
			Str("follower", follower).
			Logger(),
	}

	fc.ctx, fc.cancel = context.WithCancel(context.Background())
	fc.backoff = common.NewBackOff(fc.ctx)

	fc.lastPushed.Store(ackIndex)
	fc.ackIndex.Store(ackIndex)

	var err error
	if fc.cursorAcker, err = ackTracker.NewCursorAcker(); err != nil {
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

func (fc *followerCursor) Close() error {
	fc.Lock()
	defer fc.Unlock()

	fc.closed.Store(true)
	if fc.cancel != nil {
		fc.cancel()
	}

	if fc.stream != nil {
		return fc.stream.CloseSend()
	}

	return nil
}

func (fc *followerCursor) LastPushed() int64 {
	return fc.lastPushed.Load()
}

func (fc *followerCursor) AckIndex() int64 {
	return fc.ackIndex.Load()
}

func (fc *followerCursor) run() {
	_ = backoff.RetryNotify(func() error {
		return fc.runOnce()
	}, fc.backoff, func(err error, duration time.Duration) {
		fc.log.Error().Err(err).
			Dur("retry-after", duration).
			Msg("Error while pushing entries to follower")
	})
}

func (fc *followerCursor) runOnce() error {
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

		fc.cursorAcker.Ack(res.Offset)

		fc.ackIndex.Store(res.Offset)
	}
}
