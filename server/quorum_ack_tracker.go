package server

import (
	"errors"
	"oxia/proto"
	"oxia/server/util"
	"oxia/server/wal"
	"sync"
)

// We use an uint16 bitset to keep track of followers acks.
const maxReplicationFactor = 15

var ErrorTooManyCursors = errors.New("too many cursors")

// QuorumAckTracker
// The QuorumAckTracker is responsible for keeping track of the head index and commit index of a shard
//   - Head index: the last entry written in the local WAL of the leader
//   - Commit index: the oldest entry that is considered "fully committed", as it has received the requested amount
//     of acks from the followers
//
// The quorum ack tracker is also used to block until the head index or commit index are advanced
type QuorumAckTracker interface {
	CommitIndex() wal.EntryId

	// WaitForCommitIndex
	// Waits for the specific entry id to be fully committed.
	// After that, invokes the function f
	WaitForCommitIndex(entryId wal.EntryId, f func() (*proto.WriteResponse, error)) (*proto.WriteResponse, error)

	HeadIndex() wal.EntryId

	AdvanceHeadIndex(id wal.EntryId)

	// WaitForHeadIndex
	// Waits until the specified entry is written on the wal
	WaitForHeadIndex(entryId wal.EntryId)

	NewCursorAcker() (CursorAcker, error)
}

type quorumAckTracker struct {
	sync.Mutex
	waitForHeadIndex   *sync.Cond
	waitForCommitIndex *sync.Cond

	replicationFactor uint32
	requiredAcks      uint32

	headIndex   wal.EntryId
	commitIndex wal.EntryId

	// Keep track of the number of acks that each entry has received
	// The bitset is used to handle duplicate acks from a single follower
	tracker            map[wal.EntryId]*util.BitSet
	cursorIdxGenerator int
}

type CursorAcker interface {
	Ack(entryId wal.EntryId)
}

type cursorAcker struct {
	quorumTracker *quorumAckTracker
	cursorIdx     int
}

func NewQuorumAckTracker(replicationFactor uint32, headIndex wal.EntryId) QuorumAckTracker {
	q := &quorumAckTracker{
		// Ack quorum is number of follower acks that are required to consider the entry fully committed
		// We are suing RF/2 (and not RF/2 + 1) because the leader is already storing 1 copy locally
		requiredAcks:      replicationFactor / 2,
		replicationFactor: replicationFactor,
		headIndex:         headIndex,
		commitIndex:       wal.EntryId{},
		tracker:           make(map[wal.EntryId]*util.BitSet),
	}
	q.waitForHeadIndex = sync.NewCond(q)
	q.waitForCommitIndex = sync.NewCond(q)
	return q
}

func (q *quorumAckTracker) AdvanceHeadIndex(headIndex wal.EntryId) {
	q.Lock()
	defer q.Unlock()

	if headIndex.LessOrEqual(q.headIndex) {
		return
	}

	q.headIndex = headIndex
	q.waitForHeadIndex.Broadcast()

	if q.requiredAcks == 0 {
		q.commitIndex = headIndex
		q.waitForCommitIndex.Broadcast()
	} else {
		q.tracker[headIndex] = &util.BitSet{}
	}
}

func (q *quorumAckTracker) CommitIndex() wal.EntryId {
	q.Lock()
	defer q.Unlock()
	return q.commitIndex
}

func (q *quorumAckTracker) HeadIndex() wal.EntryId {
	q.Lock()
	defer q.Unlock()
	return q.headIndex
}

func (q *quorumAckTracker) WaitForHeadIndex(entryId wal.EntryId) {
	q.Lock()
	defer q.Unlock()

	for q.headIndex.Less(entryId) {
		q.waitForHeadIndex.Wait()
	}
}

func (q *quorumAckTracker) WaitForCommitIndex(entryId wal.EntryId, f func() (*proto.WriteResponse, error)) (*proto.WriteResponse, error) {
	q.Lock()
	defer q.Unlock()

	for q.requiredAcks > 0 && q.commitIndex.Less(entryId) {
		q.waitForCommitIndex.Wait()
	}

	return f()
}

func (q *quorumAckTracker) NewCursorAcker() (CursorAcker, error) {
	q.Lock()
	defer q.Unlock()

	if uint32(q.cursorIdxGenerator) >= q.replicationFactor-1 {
		return nil, ErrorTooManyCursors
	}

	cursoridx := q.cursorIdxGenerator
	q.cursorIdxGenerator++

	return &cursorAcker{
		quorumTracker: q,
		cursorIdx:     cursoridx,
	}, nil
}

func (c *cursorAcker) Ack(entryId wal.EntryId) {
	q := c.quorumTracker
	q.Lock()
	defer q.Unlock()

	e, found := q.tracker[entryId]
	if !found {
		// The entry has already previously reached the quorum.
		// There's nothing more left to do here.
		return
	}

	// Mark that this follower has acked the entry
	e.Set(c.cursorIdx)
	if uint32(e.Count()) == q.requiredAcks {
		delete(q.tracker, entryId)

		// Advance the commit index
		q.commitIndex = entryId
		q.waitForCommitIndex.Broadcast()
	}
}
