package batch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		name             string
		linger           time.Duration
		maxSize          int
		closeImmediately bool
		expectedErr      error
	}{
		{"complete on maxRequestsPerBatch", 1 * time.Second, 1, false, nil},
		{"complete on linger", 1 * time.Millisecond, 2, false, nil},
		{"fail on close", 1 * time.Second, 2, true, ErrorShuttingDown},
	} {
		t.Run(item.name, func(t *testing.T) {
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

			if item.closeImmediately {
				err := batcher.Close()
				assert.NoError(t, err)
			}

			assert.ErrorIs(t, <-testBatch.result, item.expectedErr)

			if !item.closeImmediately {
				err := batcher.Close()
				assert.NoError(t, err)
			}
		})
	}
}
