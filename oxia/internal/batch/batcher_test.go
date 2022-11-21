package batch

import (
	"github.com/stretchr/testify/assert"
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

func (b *testBatch) Add(call any) {
	b.count++
}

func (b *testBatch) Size() int {
	return b.count
}

func (b *testBatch) Complete() {
	close(b.result)
}

func (b *testBatch) Fail(err error) {
	b.result <- err
	//closeC(b.result)
}

func TestBatcher(t *testing.T) {
	for _, item := range []struct {
		linger      time.Duration
		maxSize     int
		expectedErr error
	}{
		{1 * time.Second, 1, nil},               //completes on maxRequestsPerBatch
		{1 * time.Millisecond, 2, nil},          //completes on linger
		{1 * time.Second, 2, ErrorShuttingDown}, //fails on closeC
	} {
		testBatch := newTestBatch()

		batchFactory := func(shardId *uint32) Batch {
			return testBatch
		}

		factory := &BatcherFactory{
			Linger:              item.linger,
			MaxRequestsPerBatch: item.maxSize,
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
