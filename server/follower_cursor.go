package server

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/proto"
	"oxia/server/wal"
	"sync"
)

// AddEntriesStreamProvider
// This is a provider for the AddEntryStream Grpc handler
// It's used to allow passing in a mocked version of the Grpc service
type AddEntriesStreamProvider interface {
	GetAddEntriesStream(follower string) (proto.OxiaLogReplication_AddEntriesClient, error)
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
	lastPushed  int64
	ackIndex    int64
	shardId     uint32

	closed bool
	log    zerolog.Logger
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
		lastPushed:               ackIndex,
		ackIndex:                 ackIndex,

		log: log.With().
			Str("component", "follower-cursor").
			Uint32("shard", shardId).
			Int64("epoch", epoch).
			Str("follower", follower).
			Logger(),
	}

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

	fc.closed = true

	if fc.stream != nil {
		return fc.stream.CloseSend()
	}

	return nil
}

func (fc *followerCursor) LastPushed() int64 {
	fc.Lock()
	defer fc.Unlock()
	return fc.lastPushed
}

func (fc *followerCursor) AckIndex() int64 {
	fc.Lock()
	defer fc.Unlock()
	return fc.ackIndex
}

func (fc *followerCursor) run() {
	for {
		fc.Lock()
		closed := fc.closed
		fc.Unlock()

		if closed {
			return
		}

		if err := fc.runOnce(); err != nil {
			fc.log.Error().Err(err).
				Msg("Error while pushing entries to follower")

			// TODO: Add exponential backoff strategy
		}
	}
}

func (fc *followerCursor) runOnce() error {
	fc.Lock()
	var err error
	if fc.stream, err = fc.addEntriesStreamProvider.GetAddEntriesStream(fc.follower); err != nil {
		return err
	}

	currentOffset := fc.ackIndex
	fc.Unlock()

	reader, err := fc.wal.NewReader(currentOffset)
	if err != nil {
		return err
	}
	defer reader.Close()

	go common.DoWithLabels(map[string]string{
		"oxia":  "follower-cursor-receive",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() {
		fc.receiveAcks(fc.stream)
	})

	fc.log.Info().
		Interface("ack-index", currentOffset).
		Msg("Successfully attached cursor follower")

	for {
		fc.Lock()
		closed := fc.closed
		fc.Unlock()

		if closed {
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

		if err = fc.stream.Send(&proto.AddEntryRequest{
			Epoch:       fc.epoch,
			Entry:       le,
			CommitIndex: fc.ackTracker.CommitIndex(),
		}); err != nil {
			return err
		}

		fc.Lock()
		fc.lastPushed = le.Offset
		currentOffset = fc.lastPushed
		fc.Unlock()
	}
}

func (fc *followerCursor) receiveAcks(stream proto.OxiaLogReplication_AddEntriesClient) {
	for {
		res, err := stream.Recv()
		if err != nil {
			fc.log.Warn().Err(err).
				Msg("Error while receiving acks")
			if err := stream.CloseSend(); err != nil {
				fc.log.Warn().Err(err).
					Msg("Error while closing stream")
			}
			return
		}

		if res == nil {
			// Stream was closed by server side
			return
		}

		if res.InvalidEpoch {
			fc.log.Error().Err(err).
				Msg("Invalid epoch")
			if err := stream.CloseSend(); err != nil {
				fc.log.Warn().Err(err).
					Msg("Error while closing stream")
			}
			return
		}

		fc.cursorAcker.Ack(res.Offset)

		fc.Lock()
		fc.ackIndex = res.Offset
		fc.Unlock()
	}
}
