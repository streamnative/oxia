package server

import (
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"oxia/server/wal"
	"testing"
	"time"
)

func TestQuorumAckTrackerNoFollower(t *testing.T) {
	at := NewQuorumAckTracker(1, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	at.AdvanceHeadIndex(5)
	assert.EqualValues(t, 5, at.HeadIndex())
	assert.EqualValues(t, 5, at.CommitIndex())

	at.AdvanceHeadIndex(6)
	assert.EqualValues(t, 6, at.HeadIndex())
	assert.EqualValues(t, 6, at.CommitIndex())

	// Head index cannot go back in time
	at.AdvanceHeadIndex(2)
	assert.EqualValues(t, 6, at.HeadIndex())
	assert.EqualValues(t, 6, at.CommitIndex())
}

func TestQuorumAckTrackerRF2(t *testing.T) {
	at := NewQuorumAckTracker(2, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	at.AdvanceHeadIndex(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c1.Ack(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.EqualValues(t, 2, at.CommitIndex())
}

func TestQuorumAckTrackerRF3(t *testing.T) {
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	at.AdvanceHeadIndex(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c2, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c1.Ack(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.EqualValues(t, 2, at.CommitIndex())

	c2.Ack(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.EqualValues(t, 2, at.CommitIndex())
}

func TestQuorumAckTrackerRF5(t *testing.T) {
	at := NewQuorumAckTracker(5, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	at.AdvanceHeadIndex(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c2, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c3, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c4, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c1.Ack(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	c2.Ack(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.EqualValues(t, 2, at.CommitIndex())

	c3.Ack(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.EqualValues(t, 2, at.CommitIndex())

	c4.Ack(2)
	assert.EqualValues(t, 2, at.HeadIndex())
	assert.EqualValues(t, 2, at.CommitIndex())
}

func TestQuorumAckTrackerMaxCursors(t *testing.T) {
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	assert.NotNil(t, c1)

	c2, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	assert.NotNil(t, c2)

	c3, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.ErrorIs(t, err, ErrorTooManyCursors)
	assert.Nil(t, c3)
}

func TestQuorumAckTracker_WaitForHeadIndex(t *testing.T) {
	at := NewQuorumAckTracker(1, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadIndex())

	ch := make(chan bool)

	go func() {
		at.WaitForHeadIndex(4)
		ch <- true
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case <-ch:
		assert.Fail(t, "should not be ready")
	default:
		// Expected. There should be nothing in the channel
	}

	at.AdvanceHeadIndex(4)
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
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)

	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())
	at.AdvanceHeadIndex(2)
	at.AdvanceHeadIndex(3)
	at.AdvanceHeadIndex(4)
	assert.Equal(t, wal.InvalidOffset, at.CommitIndex())

	ch := make(chan error)

	go func() {
		_, err := at.WaitForCommitIndex(2, func() (*proto.WriteResponse, error) {
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

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	assert.NotNil(t, c1)
	c1.Ack(2)

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

	assert.EqualValues(t, 2, at.CommitIndex())
}

func TestQuorumAckTracker_AddingCursors_RF3(t *testing.T) {
	at := NewQuorumAckTracker(3, 10, 5)

	assert.EqualValues(t, 10, at.HeadIndex())
	assert.EqualValues(t, 5, at.CommitIndex())

	c, err := at.NewCursorAcker(11)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, ErrorInvalidHeadIndex)

	c1, err := at.NewCursorAcker(7)
	assert.NotNil(t, c1)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadIndex())
	assert.EqualValues(t, 7, at.CommitIndex())

	c2, err := at.NewCursorAcker(9)
	assert.NotNil(t, c2)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadIndex())
	assert.EqualValues(t, 9, at.CommitIndex())
}

func TestQuorumAckTracker_AddingCursors_RF5(t *testing.T) {
	at := NewQuorumAckTracker(5, 10, 5)

	assert.EqualValues(t, 10, at.HeadIndex())
	assert.EqualValues(t, 5, at.CommitIndex())

	c, err := at.NewCursorAcker(11)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, ErrorInvalidHeadIndex)

	c1, err := at.NewCursorAcker(7)
	assert.NotNil(t, c1)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadIndex())
	assert.EqualValues(t, 5, at.CommitIndex())

	c2, err := at.NewCursorAcker(9)
	assert.NotNil(t, c2)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadIndex())
	assert.EqualValues(t, 7, at.CommitIndex())
}
