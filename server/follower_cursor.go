package server

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
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
	LastPushed() wal.EntryId

	// AckIndex The highest entry already acknowledged by this follower
	AckIndex() wal.EntryId
}

type followerCursor struct {
	sync.Mutex

	epoch                    uint64
	follower                 string
	addEntriesStreamProvider AddEntriesStreamProvider
	stream                   proto.OxiaLogReplication_AddEntriesClient

	ackTracker  QuorumAckTracker
	cursorAcker CursorAcker
	wal         wal.Wal
	lastPushed  wal.EntryId
	ackIndex    wal.EntryId

	closed bool
	log    zerolog.Logger
}

func NewFollowerCursor(
	follower string,
	epoch uint64,
	shardId uint32,
	addEntriesStreamProvider AddEntriesStreamProvider,
	ackTracker QuorumAckTracker,
	wal wal.Wal,
	ackIndex wal.EntryId) (FollowerCursor, error) {

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
			Uint32("shardAssignment", shardId).
			Uint64("epoch", epoch).
			Str("follower", follower).
			Logger(),
	}

	var err error
	if fc.cursorAcker, err = ackTracker.NewCursorAcker(); err != nil {
		return nil, err
	}

	go fc.run()

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

func (fc *followerCursor) LastPushed() wal.EntryId {
	fc.Lock()
	defer fc.Unlock()
	return fc.lastPushed
}

func (fc *followerCursor) AckIndex() wal.EntryId {
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

	currentEntry := fc.ackIndex
	fc.Unlock()

	reader, err := fc.wal.NewReader(currentEntry)
	if err != nil {
		return err
	}
	defer reader.Close()

	go fc.receiveAcks(fc.stream)

	fc.log.Info().
		Interface("ack-index", currentEntry).
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
			fc.ackTracker.WaitForHeadIndex(wal.EntryId{
				Epoch:  currentEntry.Epoch,
				Offset: currentEntry.Offset + 1,
			})

			continue
		}

		le, err := reader.ReadNext()
		if err != nil {
			return err
		}

		if err = fc.stream.Send(&proto.AddEntryRequest{
			Epoch:       fc.epoch,
			Entry:       le,
			CommitIndex: fc.ackTracker.CommitIndex().ToProto(),
		}); err != nil {
			return err
		}

		fc.Lock()
		fc.lastPushed = wal.EntryIdFromProto(le.EntryId)
		currentEntry = fc.lastPushed
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

		if res.InvalidEpoch {
			fc.log.Error().Err(err).
				Msg("Invalid epoch")
			if err := stream.CloseSend(); err != nil {
				fc.log.Warn().Err(err).
					Msg("Error while closing stream")
			}
			return
		}

		fc.cursorAcker.Ack(wal.EntryIdFromProto(res.EntryId))

		fc.Lock()
		fc.ackIndex = wal.EntryIdFromProto(res.EntryId)
		fc.Unlock()
	}
}
