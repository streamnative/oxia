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

package batch

import (
	"sync"
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

func TestBatcherWithBufferedChannel(t *testing.T) {
	for _, item := range []struct {
		size int
	}{
		{0},
		{1},
		{4},
	} {

		count := 100

		var batches []*testBatch
		mutex := sync.Mutex{}

		wg := sync.WaitGroup{}
		wg.Add(count)
		batchFactory := func(shardId *uint32) Batch {
			b := newTestBatch()
			mutex.Lock()
			defer mutex.Unlock()
			batches = append(batches, b)
			wg.Done()
			return b
		}

		factory := &BatcherFactory{
			Linger:              1,
			MaxRequestsPerBatch: 1,
			BatcherBufferSize:   item.size,
		}
		batcher := factory.newBatcher(&shardId, batchFactory)

		go batcher.run()

		for i := 0; i < 100; i++ {
			batcher.Add(1)
		}

		wg.Wait()
		for _, b := range batches {
			assert.NoError(t, <-b.result)
			assert.Equal(t, b.count, 1)
		}

		err := batcher.Close()
		assert.NoError(t, err)
	}
}
