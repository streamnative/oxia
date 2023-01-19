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
	"oxia/common"
	"oxia/proto"
	"testing"
	"time"
)

func init() {
	common.ConfigureLogger()
}

func TestWalTrimmer(t *testing.T) {
	wf := NewInMemoryWalFactory()
	w, err := wf.NewWal(1)
	assert.NoError(t, err)

	clock := &common.MockedClock{}

	for i := int64(0); i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Epoch:     0,
			Offset:    i,
			Value:     []byte(""),
			Timestamp: uint64(i),
		}))
	}

	trimmer := NewTrimmer(1, w, 2*time.Millisecond, 10*time.Millisecond, clock)

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
