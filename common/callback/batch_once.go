// Copyright 2025 StreamNative, Inc.
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

package callback

import "sync/atomic"

var _ StreamCallback[any] = &BatchStreamOnce[any]{}

// BatchStreamOnce is a generic struct used for batch - processing stream data.
// It triggers a flush operation when the specified batch count or byte count is reached,
// and executes corresponding callbacks upon completion or error.
type BatchStreamOnce[T any] struct {
	// container is used to store the data elements to be processed.
	container []T

	// maxBatchCount represents the maximum number of elements for batch processing.
	maxBatchCount int
	// maxBatchBytes represents the maximum number of bytes for batch processing.
	maxBatchBytes int
	// totalBatchBytes records the total number of bytes in the current batch processing.
	totalBatchBytes int

	// completed is an atomic boolean value used to mark whether the processing is completed.
	// Using atomic operations ensures thread - safety in a concurrent environment.
	completed atomic.Bool
	// onFlush is a callback function that will be called when the batch - processing conditions are met.
	// It takes a slice containing the data elements to be processed and returns an error if any.
	onFlush func(container []T) error
	// onFinish is a callback function that will be called when the processing is completed or an error occurs.
	// It takes an error parameter to indicate the status of the processing.
	onFinish func(err error)
	// getBytes is a function that calculates the number of bytes of a single data element.
	getBytes func(T) int
}

// OnNext is called when a new data element is received.
func (b *BatchStreamOnce[T]) OnNext(t T) error {
	b.container = append(b.container, t)
	b.totalBatchBytes += b.getBytes(t)
	if len(b.container) >= b.maxBatchCount || b.totalBatchBytes >= b.maxBatchBytes {
		err := b.onFlush(b.container)
		if err != nil {
			return err
		}
		b.totalBatchBytes = 0
		b.container = b.container[:0]
	}
	return nil
}

// Complete is called when the stream processing is completed successfully.
func (b *BatchStreamOnce[T]) Complete(err error) {
	if !b.completed.CompareAndSwap(false, true) {
		return
	}
	if err != nil {
		b.onFinish(err)
		return
	}
	b.onFinish(b.onFlush(b.container))
}

// NewBatchStreamOnce is a factory function used to create a new BatchStreamOnce instance.
func NewBatchStreamOnce[T any](
	maxBatchCount int,
	maxBatchBytes int,
	getBytes func(T) int,
	onFlush func(container []T) error,
	onFinish func(err error)) *BatchStreamOnce[T] {

	return &BatchStreamOnce[T]{
		maxBatchCount: maxBatchCount,
		maxBatchBytes: maxBatchBytes,
		onFlush:       onFlush,
		onFinish:      onFinish,
		getBytes:      getBytes,
	}
}
