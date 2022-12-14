package server

import (
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"oxia/server/wal"
	"testing"
	"time"
)

func TestFollowerCursor(t *testing.T) {
	var epoch int64 = 1
	var shard uint32 = 2

	stream := newMockRpcClient()
	ackTracker := NewQuorumAckTracker(3, wal.InvalidOffset, wal.InvalidOffset)
	wf := wal.NewWalFactory(&wal.WalFactoryOptions{LogDir: t.TempDir()})
	w, err := wf.NewWal(shard)
	assert.NoError(t, err)

	fc, err := NewFollowerCursor("f1", epoch, shard, stream, ackTracker, w, wal.InvalidOffset)
	assert.NoError(t, err)

	assert.Equal(t, wal.InvalidOffset, fc.LastPushed())
	assert.Equal(t, wal.InvalidOffset, fc.AckIndex())

	err = w.Append(&proto.LogEntry{
		Epoch:  1,
		Offset: 0,
		Value:  []byte("v1"),
	})
	assert.NoError(t, err)

	assert.Equal(t, wal.InvalidOffset, fc.LastPushed())
	assert.Equal(t, wal.InvalidOffset, fc.AckIndex())

	ackTracker.AdvanceHeadIndex(0)

	assert.Eventually(t, func() bool {
		return fc.LastPushed() == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, wal.InvalidOffset, fc.AckIndex())

	// The follower is acking back
	req := <-stream.addEntryReqs
	assert.EqualValues(t, 1, req.Epoch)
	assert.Equal(t, wal.InvalidOffset, req.CommitIndex)

	stream.addEntryResps <- &proto.AddEntryResponse{
		Epoch:  1,
		Offset: 0,
	}

	assert.Eventually(t, func() bool {
		return fc.AckIndex() == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 0, ackTracker.CommitIndex())

	// Next entry should carry the correct commit index
	err = w.Append(&proto.LogEntry{
		Epoch:  1,
		Offset: 1,
		Value:  []byte("v2"),
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 0, fc.LastPushed())
	assert.EqualValues(t, 0, fc.AckIndex())

	ackTracker.AdvanceHeadIndex(1)

	assert.Eventually(t, func() bool {
		return fc.LastPushed() == 1
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 0, fc.AckIndex())

	req = <-stream.addEntryReqs
	assert.EqualValues(t, 1, req.Epoch)
	assert.EqualValues(t, 1, req.Entry.Epoch)
	assert.EqualValues(t, 1, req.Entry.Offset)
	assert.EqualValues(t, 0, req.CommitIndex)

	assert.NoError(t, fc.Close())
}
