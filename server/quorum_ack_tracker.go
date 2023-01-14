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

	headIndex   int64
	commitIndex int64

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
		headIndex:         headIndex,
		commitIndex:       commitIndex,
		tracker:           make(map[int64]*util.BitSet),
	}

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

	if headIndex <= q.headIndex {
		return
	}

	atomic.StoreInt64(&q.headIndex, headIndex)
	q.waitForHeadIndex.Broadcast()

	if q.requiredAcks == 0 {
		atomic.StoreInt64(&q.commitIndex, headIndex)
		q.waitForCommitIndex.Broadcast()
	} else {
		q.tracker[headIndex] = &util.BitSet{}
	}
}

func (q *quorumAckTracker) CommitIndex() int64 {
	return atomic.LoadInt64(&q.commitIndex)
}

func (q *quorumAckTracker) HeadIndex() int64 {
	return atomic.LoadInt64(&q.headIndex)
}

func (q *quorumAckTracker) WaitForHeadIndex(offset int64) {
	q.Lock()
	defer q.Unlock()

	for !q.closed && q.headIndex < offset {
		q.waitForHeadIndex.Wait()
	}
}

func (q *quorumAckTracker) WaitForCommitIndex(offset int64, f func() (*proto.WriteResponse, error)) (*proto.WriteResponse, error) {
	q.Lock()
	defer q.Unlock()

	for !q.closed && q.requiredAcks > 0 && q.commitIndex < offset {
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

	if ackIndex > q.headIndex {
		return nil, ErrorInvalidHeadIndex
	}

	qa := &cursorAcker{
		quorumTracker: q,
		cursorIdx:     q.cursorIdxGenerator,
	}

	// If the new cursor is already past the current quorum commit index, we have
	// to mark these entries as acked (by that cursor).
	for offset := q.commitIndex + 1; offset <= ackIndex; offset++ {
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
		atomic.StoreInt64(&q.commitIndex, offset)
		q.waitForCommitIndex.Broadcast()
	}
}
