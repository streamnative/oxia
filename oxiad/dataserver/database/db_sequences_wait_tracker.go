// Copyright 2023-2026 The Oxia Authors
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

package database

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/channel"
)

type SequenceWaiter interface {
	io.Closer

	Receive(context.Context) (string, error)

	Ch() <-chan string
}

type SequenceWaiterTracker interface {
	io.Closer
	// AddSequenceWaiter registers a waiter for the sequence, and sends it the last
	// key that readLastKey returns, unless SequenceUpdated sent it a key during the
	// read, so the read can't override a newer key. The waiter is registered
	// before the read, so a commit that the read misses sends its key.
	AddSequenceWaiter(key string, readLastKey func() (string, bool, error)) (*sequenceWaiter, error)
	SequenceUpdated(prefixKey string, lastSequenceKey string)
}

type sequenceWaiterID int64

type sequenceWaiterTracker struct {
	sync.RWMutex
	waiters map[string]map[sequenceWaiterID]*sequenceWaiter
	idGen   atomic.Int64
}

type sequenceWaiter struct {
	key     string
	id      sequenceWaiterID
	och     channel.OverrideChannel[string]
	tracker *sequenceWaiterTracker

	closed atomic.Bool
	// updated is set once SequenceUpdated sends the waiter a key
	updated atomic.Bool
}

func (sw *sequenceWaiter) Receive(ctx context.Context) (string, error) {
	return sw.och.Receive(ctx)
}

func (sw *sequenceWaiter) Ch() <-chan string {
	return sw.och.Ch()
}

func (sw *sequenceWaiter) closeWithoutLock() {
	if !sw.closed.CompareAndSwap(false, true) {
		return
	}
	sw.tracker.removeWithoutLock(sw.key, sw.id)
	close(sw.och.Ch())
}

func (sw *sequenceWaiter) Close() error {
	if !sw.closed.CompareAndSwap(false, true) {
		return nil
	}
	sw.tracker.remove(sw.key, sw.id)
	close(sw.och.Ch())
	return nil
}

// ////////////////////////////////////////////////////////////////////////////

func NewSequencesWaitTracker() SequenceWaiterTracker {
	return &sequenceWaiterTracker{
		waiters: map[string]map[sequenceWaiterID]*sequenceWaiter{},
	}
}

func (swt *sequenceWaiterTracker) AddSequenceWaiter(key string,
	readLastKey func() (string, bool, error)) (*sequenceWaiter, error) {
	swt.Lock()
	im, existing := swt.waiters[key]
	if !existing {
		im = map[sequenceWaiterID]*sequenceWaiter{}
		swt.waiters[key] = im
	}

	id := sequenceWaiterID(swt.idGen.Add(1))
	sw := &sequenceWaiter{key: key, id: id, och: channel.NewOverrideChannel[string](), tracker: swt}
	im[id] = sw
	swt.Unlock()

	// Registered first: a commit that the read misses sends its key
	lastKey, found, err := readLastKey()
	if err != nil {
		_ = sw.Close()
		return nil, err
	}
	if found {
		swt.Lock()
		// Once SequenceUpdated sends a key, it sends the key of every later
		// commit too: the waiter gets the read's key, or a newer one, anyway
		if !sw.updated.Load() && !sw.closed.Load() {
			sw.och.WriteLast(lastKey)
		}
		swt.Unlock()
	}
	return sw, nil
}

func (swt *sequenceWaiterTracker) removeWithoutLock(key string, id sequenceWaiterID) {
	// The waiters of the key are gone when the tracker closed first
	im := swt.waiters[key]
	delete(im, id)
	if len(im) == 0 {
		delete(swt.waiters, key)
	}
}

func (swt *sequenceWaiterTracker) remove(key string, id sequenceWaiterID) {
	swt.Lock()
	defer swt.Unlock()
	swt.removeWithoutLock(key, id)
}

func (swt *sequenceWaiterTracker) SequenceUpdated(prefixKey string, lastSequenceKey string) {
	swt.RLock()
	defer swt.RUnlock()

	for _, w := range swt.waiters[prefixKey] {
		w.updated.Store(true)
		w.och.WriteLast(lastSequenceKey)
	}
}

func (swt *sequenceWaiterTracker) Close() error {
	swt.Lock()
	defer swt.Unlock()

	for _, m := range swt.waiters {
		for _, w := range m {
			w.closeWithoutLock()
		}

		clear(m)
	}

	clear(swt.waiters)
	return nil
}
