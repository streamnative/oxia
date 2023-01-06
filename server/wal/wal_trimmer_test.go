package wal

import (
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"oxia/proto"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	common.ConfigureLogger()
}

type mockClock struct {
	currentTime atomic.Int64
}

func (c *mockClock) Now() time.Time {
	return time.UnixMilli(c.currentTime.Load())
}

func TestWalTrimmer(t *testing.T) {
	wf := NewInMemoryWalFactory()
	w, err := wf.NewWal(1)
	assert.NoError(t, err)

	clock := &mockClock{}

	for i := int64(0); i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Epoch:     0,
			Offset:    i,
			Value:     []byte(""),
			Timestamp: uint64(i),
		}))
	}

	trimmer := NewTrimmer(1, w, 2*time.Millisecond, 10*time.Millisecond, clock)

	clock.currentTime.Store(2)

	// Should not get triggered since there are not expired entries yet
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	clock.currentTime.Store(5)

	assert.Eventually(t, func() bool {
		log.Info().Int64("first-offset", w.FirstOffset()).Msg("checking...")
		return w.FirstOffset() == 3
	}, 10*time.Second, 10*time.Millisecond)

	clock.currentTime.Store(89)

	assert.Eventually(t, func() bool {
		log.Info().Int64("first-offset", w.FirstOffset()).Msg("checking...")
		return w.FirstOffset() == 87
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, trimmer.Close())

	clock.currentTime.Store(95)

	// The trimmer should not be operating anymore once it's closed
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, 87, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())
}
