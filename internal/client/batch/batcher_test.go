package batch

import (
	"github.com/stretchr/testify/assert"
	"oxia/oxia"
	"sync"
	"testing"
	"time"
)

type testBatch struct {
	count  int
	calls  []any
	result chan error
}

func newTestBatch() *testBatch {
	return &testBatch{
		calls:  make([]any, 0),
		result: make(chan error, 1),
	}
}

func (b *testBatch) add(call any) {
	b.count++
}

func (b *testBatch) size() int {
	return b.count
}

func (b *testBatch) complete() {
	close(b.result)
}

func (b *testBatch) fail(err error) {
	b.result <- err
	//close(b.result)
}

func TestBatcher(t *testing.T) {
	for _, item := range []struct {
		linger      time.Duration
		maxSize     int
		expectedErr error
	}{
		{1 * time.Second, 1, nil},                    //completes on maxSize
		{1 * time.Millisecond, 2, nil},               //completes on linger
		{1 * time.Second, 2, oxia.ErrorShuttingDown}, //fails on close
	} {
		testBatch := newTestBatch()

		batchFactory := func(shardId *uint32) batch {
			return testBatch
		}

		factory := &BatcherFactory{
			Linger:  item.linger,
			MaxSize: item.maxSize,
		}
		batcher := factory.newBatcher(&shardId, batchFactory)

		go batcher.run()

		batcher.Add(1)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			assert.Equal(t, item.expectedErr, <-testBatch.result)
			wg.Done()
		}()

		time.Sleep(1 * time.Millisecond)

		err := batcher.Close()
		assert.ErrorIs(t, nil, err)

		wg.Wait()
	}
}
