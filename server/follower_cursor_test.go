package server

import (
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"oxia/server/wal"
	"testing"
	"time"
)

func TestFollowerCursor(t *testing.T) {
	var epoch uint64 = 1
	var shard uint32 = 2

	stream := newMockClientAddEntriesStream()
	ackTracker := NewQuorumAckTracker(3, wal.EntryId{Epoch: 0, Offset: 0})
	wf := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})
	w, err := wf.NewWal(shard)
	assert.NoError(t, err)

	fc, err := NewFollowerCursor("f1", epoch, shard, stream, ackTracker, w, wal.EntryId{})
	assert.NoError(t, err)

	assert.Equal(t, wal.EntryId{}, fc.LastPushed())
	assert.Equal(t, wal.EntryId{}, fc.AckIndex())

	err = w.Append(&proto.LogEntry{
		EntryId:   &proto.EntryId{Epoch: 1, Offset: 0},
		Value:     []byte("v1"),
		Timestamp: 0,
	})
	assert.NoError(t, err)

	assert.Equal(t, wal.EntryId{}, fc.LastPushed())
	assert.Equal(t, wal.EntryId{}, fc.AckIndex())

	ackTracker.AdvanceHeadIndex(wal.EntryId{Epoch: 1, Offset: 0})

	assert.Eventually(t, func() bool {
		return fc.LastPushed().Equal(wal.EntryId{Epoch: 1, Offset: 0})
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, wal.EntryId{}, fc.AckIndex())

	// The follower is acking back
	req := <-stream.requests
	assert.EqualValues(t, 1, req.Epoch)
	assert.Equal(t, &proto.EntryId{Epoch: 0, Offset: 0}, req.CommitIndex)

	stream.responses <- &proto.AddEntryResponse{
		Epoch:        1,
		EntryId:      &proto.EntryId{Epoch: 1, Offset: 0},
		InvalidEpoch: false,
	}

	assert.Eventually(t, func() bool {
		return fc.AckIndex().Equal(wal.EntryId{Epoch: 1, Offset: 0})
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, wal.EntryId{Epoch: 1, Offset: 0}, ackTracker.CommitIndex())

	// Next entry should carry the correct commit index
	err = w.Append(&proto.LogEntry{
		EntryId:   &proto.EntryId{Epoch: 1, Offset: 1},
		Value:     []byte("v2"),
		Timestamp: 0,
	})
	assert.NoError(t, err)

	assert.Equal(t, wal.EntryId{Epoch: 1, Offset: 0}, fc.LastPushed())
	assert.Equal(t, wal.EntryId{Epoch: 1, Offset: 0}, fc.AckIndex())

	ackTracker.AdvanceHeadIndex(wal.EntryId{Epoch: 1, Offset: 1})

	assert.Eventually(t, func() bool {
		return fc.LastPushed().Equal(wal.EntryId{Epoch: 1, Offset: 1})
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, wal.EntryId{Epoch: 1, Offset: 0}, fc.AckIndex())

	req = <-stream.requests
	assert.EqualValues(t, 1, req.Epoch)
	assert.EqualValues(t, 1, req.Entry.EntryId.Epoch)
	assert.EqualValues(t, 1, req.Entry.EntryId.Offset)
	assert.Equal(t, &proto.EntryId{Epoch: 1, Offset: 0}, req.CommitIndex)

	assert.NoError(t, fc.Close())
}
