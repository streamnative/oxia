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
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

var closeErr = errors.New("closed")

type testBatcher struct {
	closed bool
}

func (b *testBatcher) Close() error {
	b.closed = true
	return closeErr
}

func (b *testBatcher) Add(any) {}

func (b *testBatcher) run() {}

func TestManager(t *testing.T) {
	testBatcher := &testBatcher{}

	newBatcherInvocations := 0
	batcherFactory := func(*uint32) Batcher {
		newBatcherInvocations++
		return testBatcher
	}

	manager := NewManager(batcherFactory)

	batcher := manager.Get(shardId)
	assert.Equal(t, testBatcher, batcher)
	assert.Equal(t, 1, newBatcherInvocations)

	batcher = manager.Get(shardId)
	assert.Equal(t, testBatcher, batcher)
	assert.Equal(t, 1, newBatcherInvocations)

	err := manager.Close()
	assert.ErrorIs(t, err, closeErr)

	assert.True(t, testBatcher.closed)

	_ = manager.Get(shardId)
	//proves that the batcher was removed on Close
	//as it had to recreate it on Get
	assert.Equal(t, 2, newBatcherInvocations)
}
