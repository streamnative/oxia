package server

import (
	"errors"
	"io"
	"oxia/common"
	"oxia/proto"
	"oxia/server/util"
	"sync"
	"sync/atomic"
)

var (
	ErrorTooManyCursors   = errors.New("too many cursors")
	ErrorInvalidHeadIndex = errors.New("invalid head index")
)

// QuorumAckTracker
// The QuorumAckTracker is responsible for keeping track of the head index and commit index of a shard
//   - Head index: the last entry written in the local WAL of the leader
//   - Commit index: the oldest entry that is considered "fully committed", as it has received the requested amount
//     of acks from the followers
//
// The quorum ack tracker is also used to block until the head index or commit index are advanced
type QuorumAckTracker interface {
	io.Closer

	CommitIndex() int64

	// WaitForCommitIndex
	// Waits for the specific entry id to be fully committed.
	// After that, invokes the function f
	WaitForCommitIndex(offset int64, f func() (*proto.WriteResponse, error)) (*proto.WriteResponse, error)

	// NextOffset returns the offset for the next entry to write
	// Note this can go ahead of the head-index as there can be multiple operations in flight.
	NextOffset() int64

	HeadIndex() int64

	AdvanceHeadIndex(headIndex int64)

	// WaitForHeadIndex
	// Waits until the specified entry is written on the wal
	WaitForHeadIndex(offset int64)

	// NewCursorAcker creates a tracker for a new cursor
	// The `ackIndex` is the previous last-acked position for the cursor
	NewCursorAcker(ackIndex int64) (CursorAcker, error)
}

type quorumAckTracker struct {
	sync.Mutex
	waitForHeadIndex   *sync.Cond
	waitForCommitIndex *sync.Cond

	replicationFactor uint32
	requiredAcks      uint32

	nextOffset  atomic.Int64
	headIndex   atomic.Int64
	commitIndex atomic.Int64

	// Keep track of the number of acks that each entry has received
	// The bitset is used to handle duplicate acks from a single follower
	tracker            map[int64]*util.BitSet
	cursorIdxGenerator int
	closed             bool
}

type CursorAcker interface {
	Ack(offset int64)
}

type cursorAcker struct {
	quorumTracker *quorumAckTracker
	cursorIdx     int
}

func NewQuorumAckTracker(replicationFactor uint32, headIndex int64, commitIndex int64) QuorumAckTracker {
	q := &quorumAckTracker{
		// Ack quorum is number of follower acks that are required to consider the entry fully committed
		// We are using RF/2 (and not RF/2 + 1) because the leader is already storing 1 copy locally
		requiredAcks:      replicationFactor / 2,
		replicationFactor: replicationFactor,
		tracker:           make(map[int64]*util.BitSet),
	}

	q.nextOffset.Store(headIndex)
	q.headIndex.Store(headIndex)
	q.commitIndex.Store(commitIndex)

	// Add entries to track the entries we're not yet sure that are fully committed
	for offset := commitIndex + 1; offset <= headIndex; offset++ {
		q.tracker[offset] = &util.BitSet{}
	}

	q.waitForHeadIndex = sync.NewCond(q)
	q.waitForCommitIndex = sync.NewCond(q)
	return q
}

func (q *quorumAckTracker) AdvanceHeadIndex(headIndex int64) {
	q.Lock()
	defer q.Unlock()

	if headIndex <= q.headIndex.Load() {
		return
	}

	q.headIndex.Store(headIndex)
	q.waitForHeadIndex.Broadcast()

	if q.requiredAcks == 0 {
		q.commitIndex.Store(headIndex)
		q.waitForCommitIndex.Broadcast()
	} else {
		q.tracker[headIndex] = &util.BitSet{}
	}
}

func (q *quorumAckTracker) NextOffset() int64 {
	return q.nextOffset.Add(1)
}

func (q *quorumAckTracker) CommitIndex() int64 {
	return q.commitIndex.Load()
}

func (q *quorumAckTracker) HeadIndex() int64 {
	return q.headIndex.Load()
}

func (q *quorumAckTracker) WaitForHeadIndex(offset int64) {
	q.Lock()
	defer q.Unlock()

	for !q.closed && q.headIndex.Load() < offset {
		q.waitForHeadIndex.Wait()
	}
}

func (q *quorumAckTracker) WaitForCommitIndex(offset int64, f func() (*proto.WriteResponse, error)) (*proto.WriteResponse, error) {
	q.Lock()
	defer q.Unlock()

	for !q.closed && q.requiredAcks > 0 && q.commitIndex.Load() < offset {
		q.waitForCommitIndex.Wait()
	}

	if q.closed {
		return nil, common.ErrorAlreadyClosed
	}

	if f != nil {
		return f()
	}

	return nil, nil
}

func (q *quorumAckTracker) Close() error {
	q.Lock()
	defer q.Unlock()

	q.closed = true
	q.waitForCommitIndex.Broadcast()
	q.waitForHeadIndex.Broadcast()
	return nil
}

func (q *quorumAckTracker) NewCursorAcker(ackIndex int64) (CursorAcker, error) {
	q.Lock()
	defer q.Unlock()

	if uint32(q.cursorIdxGenerator) >= q.replicationFactor-1 {
		return nil, ErrorTooManyCursors
	}

	if ackIndex > q.headIndex.Load() {
		return nil, ErrorInvalidHeadIndex
	}

	qa := &cursorAcker{
		quorumTracker: q,
		cursorIdx:     q.cursorIdxGenerator,
	}

	// If the new cursor is already past the current quorum commit index, we have
	// to mark these entries as acked (by that cursor).
	for offset := q.commitIndex.Load() + 1; offset <= ackIndex; offset++ {
		qa.ack(offset)
	}

	q.cursorIdxGenerator++
	return qa, nil
}

func (c *cursorAcker) Ack(offset int64) {
	c.quorumTracker.Lock()
	defer c.quorumTracker.Unlock()

	c.ack(offset)
}

func (c *cursorAcker) ack(offset int64) {
	q := c.quorumTracker

	e, found := q.tracker[offset]
	if !found {
		// The entry has already previously reached the quorum.
		// There's nothing more left to do here.
		return
	}

	// Mark that this follower has acked the entry
	e.Set(c.cursorIdx)
	if uint32(e.Count()) == q.requiredAcks {
		delete(q.tracker, offset)

		// Advance the commit index
		q.commitIndex.Store(offset)
		q.waitForCommitIndex.Broadcast()
	}
}
