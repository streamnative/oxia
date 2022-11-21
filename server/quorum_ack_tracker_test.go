package server

import (
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"oxia/server/wal"
	"testing"
	"time"
)

func TestQuorumAckTrackerNoFollower(t *testing.T) {
	at := NewQuorumAckTracker(1, wal.EntryId{1, 1})

	assert.Equal(t, wal.EntryId{1, 1}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	at.AdvanceHeadIndex(wal.EntryId{1, 5})
	assert.Equal(t, wal.EntryId{1, 5}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 5}, at.CommitIndex())

	at.AdvanceHeadIndex(wal.EntryId{1, 6})
	assert.Equal(t, wal.EntryId{1, 6}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 6}, at.CommitIndex())

	// Head index cannot go back in time
	at.AdvanceHeadIndex(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 6}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 6}, at.CommitIndex())
}

func TestQuorumAckTrackerRF2(t *testing.T) {
	at := NewQuorumAckTracker(2, wal.EntryId{1, 1})

	assert.Equal(t, wal.EntryId{1, 1}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	at.AdvanceHeadIndex(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	c1, err := at.NewCursorAcker()
	assert.NoError(t, err)

	c1.Ack(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 2}, at.CommitIndex())
}

func TestQuorumAckTrackerRF3(t *testing.T) {
	at := NewQuorumAckTracker(3, wal.EntryId{1, 1})

	assert.Equal(t, wal.EntryId{1, 1}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	at.AdvanceHeadIndex(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	c1, err := at.NewCursorAcker()
	assert.NoError(t, err)

	c2, err := at.NewCursorAcker()
	assert.NoError(t, err)

	c1.Ack(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 2}, at.CommitIndex())

	c2.Ack(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 2}, at.CommitIndex())
}

func TestQuorumAckTrackerRF5(t *testing.T) {
	at := NewQuorumAckTracker(5, wal.EntryId{1, 1})

	assert.Equal(t, wal.EntryId{1, 1}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	at.AdvanceHeadIndex(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	c1, err := at.NewCursorAcker()
	assert.NoError(t, err)

	c2, err := at.NewCursorAcker()
	assert.NoError(t, err)

	c3, err := at.NewCursorAcker()
	assert.NoError(t, err)

	c4, err := at.NewCursorAcker()
	assert.NoError(t, err)

	c1.Ack(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	c2.Ack(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 2}, at.CommitIndex())

	c3.Ack(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 2}, at.CommitIndex())

	c4.Ack(wal.EntryId{1, 2})
	assert.Equal(t, wal.EntryId{1, 2}, at.HeadIndex())
	assert.Equal(t, wal.EntryId{1, 2}, at.CommitIndex())
}

func TestQuorumAckTrackerMaxCursors(t *testing.T) {
	at := NewQuorumAckTracker(3, wal.EntryId{1, 1})

	c1, err := at.NewCursorAcker()
	assert.NoError(t, err)
	assert.NotNil(t, c1)

	c2, err := at.NewCursorAcker()
	assert.NoError(t, err)
	assert.NotNil(t, c2)

	c3, err := at.NewCursorAcker()
	assert.ErrorIs(t, err, ErrorTooManyCursors)
	assert.Nil(t, c3)
}

func TestQuorumAckTracker_WaitForHeadIndex(t *testing.T) {
	at := NewQuorumAckTracker(1, wal.EntryId{1, 1})

	assert.Equal(t, wal.EntryId{1, 1}, at.HeadIndex())

	ch := make(chan bool)

	go func() {
		at.WaitForHeadIndex(wal.EntryId{1, 4})
		ch <- true
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case <-ch:
		assert.Fail(t, "should not be ready")
	default:
		// Expected. There should be nothing in the channel
	}

	at.AdvanceHeadIndex(wal.EntryId{1, 4})
	assert.Eventually(t, func() bool {
		select {
		case <-ch:
			// Expected, the operation should be already done
			return true
		default:
			// Should have been ready
			return false
		}
	}, 10*time.Second, 100*time.Millisecond)

}

func TestQuorumAckTracker_WaitForCommitIndex(t *testing.T) {
	at := NewQuorumAckTracker(3, wal.EntryId{1, 1})

	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())
	at.AdvanceHeadIndex(wal.EntryId{1, 2})
	at.AdvanceHeadIndex(wal.EntryId{1, 3})
	at.AdvanceHeadIndex(wal.EntryId{1, 4})
	assert.Equal(t, wal.EntryId{0, 0}, at.CommitIndex())

	ch := make(chan error)

	go func() {
		_, err := at.WaitForCommitIndex(wal.EntryId{1, 2}, func() (*proto.WriteResponse, error) {
			return nil, nil
		})
		ch <- err
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case <-ch:
		assert.Fail(t, "should not be ready")
	default:
		// Expected. There should be nothing in the channel
	}

	c1, err := at.NewCursorAcker()
	assert.NoError(t, err)
	assert.NotNil(t, c1)
	c1.Ack(wal.EntryId{1, 2})

	assert.Eventually(t, func() bool {
		select {
		case <-ch:
			// Expected, the operation should be already done
			return true
		default:
			// Should have been ready
			return false
		}
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, wal.EntryId{1, 2}, at.CommitIndex())
}
