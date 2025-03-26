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

// Callback defines an interface for handling the completion of an asynchronous operation.
// It allows the caller to notify the system when an operation has completed successfully
// or when it has failed with an error. The interface provides two methods for handling
// both success and error cases.
//
// The generic type T represents the result type of the operation, which can vary depending
// on the specific use case.
//
// Methods:
//
//   - Complete(t T): This method is called when the asynchronous operation completes successfully.
//     It accepts a result of type T, which is the outcome of the operation.
//
//   - CompleteError(err error): This method is called when the asynchronous operation fails.
//     It accepts an error, which indicates the reason for the failure.
type Callback[T any] interface {
	// Complete is invoked when the operation completes successfully with the result 't' of type T.
	Complete(t T)

	// CompleteError is invoked when the operation fails, providing an error 'err' indicating the failure reason.
	CompleteError(err error)
}

// StreamCallback is a generic interface designed to handle asynchronous stream - related operations.
// It provides a set of methods to respond to different events during the stream processing.
// Implementations of this interface should ensure that the methods are used appropriately
// according to the stream's state.
type StreamCallback[T any] interface {
	// OnNext is called when a new item is available in the stream.
	// It takes an item of type T as an argument and processes it.
	// This method can be called multiple times as long as there are items in the stream.
	OnNext(t T) error

	// Complete is invoked when the stream has been fully processed without errors.
	// It indicates the successful end of the stream processing.
	// After this method is called, no further calls to OnNext or CompleteError should be expected.
	Complete()

	// CompleteError is triggered when an error occurs during the stream processing.
	// It accepts an error object that describes the nature of the problem.
	// Once this method is called, the stream processing is considered to have failed,
	// and no more calls to OnNext or Complete will be made.
	CompleteError(err error)
}
