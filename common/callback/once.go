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

// Once ensures that a specific callback is executed only once, either on completion or on error.
// It prevents further execution of the callbacks after the first call and ensures atomicity
// of the operation using the atomic package.
//
// The generic type T represents the result type of the operation, and the callbacks
// provide the behavior for handling success or failure of the operation.
//
// Fields:
// - OnComplete: A function that gets called with the result of type T when the operation completes successfully.
// - OnCompleteError: A function that gets called with an error if the operation fails.
// - completed: An atomic boolean used to track if the operation has already completed, ensuring only one callback is executed.

type Once[T any] struct {
	OnComplete      func(t T)       // Callback function called on successful completion
	OnCompleteError func(err error) // Callback function called when an error occurs
	completed       atomic.Bool     // Atomic flag to track completion status
}

// Complete is called to notify that the operation has completed successfully with the result 't'.
// It ensures that the 'OnComplete' callback is only called once.
func (c *Once[T]) Complete(t T) {
	if !c.completed.CompareAndSwap(false, true) {
		return
	}
	c.OnComplete(t)
}

// CompleteError is called to notify that the operation has failed with an error 'err'.
// It ensures that the 'OnCompleteError' callback is only called once.
func (c *Once[T]) CompleteError(err error) {
	if !c.completed.CompareAndSwap(false, true) {
		return
	}
	c.OnCompleteError(err)
}

// NewOnce creates a new instance of Once with the provided success and error callbacks.
// It ensures that the callbacks are invoked only once, either for success or failure.
func NewOnce[T any](onComplete func(t T), onError func(err error)) Callback[T] {
	return &Once[T]{
		onComplete,
		onError,
		atomic.Bool{},
	}
}

// OnceStream is a generic struct designed to handle callbacks for streaming data.
// It offers three methods: OnNext, Complete, and CompleteError.
// The OnNext method processes each data item in the stream,
// the Complete method marks the successful completion of the stream,
// and the CompleteError method marks the stream's completion due to an error.
//
// Warning: There is a race condition in this implementation, which may allow the OnNext method
// to be called after the Complete or CompleteError method has been invoked.
// To avoid introducing locks or other high - overhead synchronization mechanisms,
// the caller is responsible for ensuring that this does not happen.
type OnceStream[T any] struct {
	onNext          func(t T) error
	onComplete      func()
	onCompleteError func(err error)
	completed       atomic.Bool
}

// OnNext processes the next data item in the stream.
// Note: There is a potential race condition where this method might be called after the stream is completed.
// The caller should ensure that this method is not called after the stream is marked as completed.
// This method provides a best - effort check to avoid processing data after completion.
func (o *OnceStream[T]) OnNext(t T) error {
	if o.completed.Load() {
		return nil
	}
	return o.onNext(t)
}

// CompleteError marks the stream as completed due to an error.
// It uses an atomic compare - and - swap operation to ensure that this method is called only once.
// If the stream has already been marked as completed, this method will do nothing.
func (o *OnceStream[T]) CompleteError(err error) {
	if !o.completed.CompareAndSwap(false, true) {
		return
	}
	o.onCompleteError(err)
}

// Complete marks the stream as successfully completed.
// It uses an atomic compare - and - swap operation to ensure that this method is called only once.
// If the stream has already been marked as completed, this method will do nothing.
func (o *OnceStream[T]) Complete() {
	if !o.completed.CompareAndSwap(false, true) {
		return
	}
	o.onComplete()
}

func NewOnceStream[T any](onNext func(t T) error, onComplete func(), onError func(err error)) StreamCallback[T] {
	return &OnceStream[T]{
		onNext,
		onComplete,
		onError,
		atomic.Bool{},
	}
}
