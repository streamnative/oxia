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

package wal

import (
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"math"
	"oxia/common"
	"oxia/proto"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	common.ConfigureLogger()
}

type mockedCommitOffsetProvider struct {
	commitOffset atomic.Int64
}

func (p *mockedCommitOffsetProvider) CommitOffset() int64 {
	return p.commitOffset.Load()
}

func TestWalTrimmer(t *testing.T) {
	wf := NewInMemoryWalFactory()
	w, err := wf.NewWal(common.DefaultNamespace, 1)
	assert.NoError(t, err)

	clock := &common.MockedClock{}
	commitOffsetProvider := &mockedCommitOffsetProvider{}
	commitOffsetProvider.commitOffset.Store(math.MaxInt64)

	for i := int64(0); i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:      0,
			Offset:    i,
			Value:     []byte(""),
			Timestamp: uint64(i),
		}))
	}

	trimmer := NewTrimmer(common.DefaultNamespace, 1, w, 2*time.Millisecond, 10*time.Millisecond, clock, commitOffsetProvider)

	clock.Set(2)

	// Should not get triggered since there are not expired entries yet
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	clock.Set(5)

	assert.Eventually(t, func() bool {
		log.Info().Int64("first-offset", w.FirstOffset()).Msg("checking...")
		return w.FirstOffset() == 3
	}, 10*time.Second, 10*time.Millisecond)

	clock.Set(89)

	assert.Eventually(t, func() bool {
		log.Info().Int64("first-offset", w.FirstOffset()).Msg("checking...")
		return w.FirstOffset() == 87
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, trimmer.Close())

	clock.Set(95)

	// The trimmer should not be operating anymore once it's closed
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, 87, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())
}

func TestWalTrimUpToCommitOffset(t *testing.T) {
	wf := NewInMemoryWalFactory()
	w, err := wf.NewWal(common.DefaultNamespace, 1)
	assert.NoError(t, err)

	clock := &common.MockedClock{}
	commitOffsetProvider := &mockedCommitOffsetProvider{}
	commitOffsetProvider.commitOffset.Store(-1)

	for i := int64(0); i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:      0,
			Offset:    i,
			Value:     []byte(""),
			Timestamp: uint64(i),
		}))
	}

	trimmer := NewTrimmer(common.DefaultNamespace, 1, w, 2*time.Millisecond, 10*time.Millisecond, clock, commitOffsetProvider)

	clock.Set(5)
	time.Sleep(100 * time.Microsecond)

	// No trimming should happen yet, because of commit offset
	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	commitOffsetProvider.commitOffset.Store(2)

	assert.Eventually(t, func() bool {
		log.Info().Int64("first-offset", w.FirstOffset()).Msg("checking...")
		return w.FirstOffset() == 2
	}, 10*time.Second, 10*time.Millisecond)

	clock.Set(89)

	time.Sleep(100 * time.Microsecond)

	// No trimming should happen yet, because of commit offset
	assert.EqualValues(t, 2, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	commitOffsetProvider.commitOffset.Store(100)

	assert.Eventually(t, func() bool {
		log.Info().Int64("first-offset", w.FirstOffset()).Msg("checking...")
		return w.FirstOffset() == 87
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, trimmer.Close())
}
