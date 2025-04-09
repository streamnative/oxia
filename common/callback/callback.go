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

// StreamCallback is a generic interface designed to handle streaming data.
// It provides methods to process each incoming data item and to signal the completion of the stream,
// which can be used in scenarios where data is processed in a streaming fashion,
// such as handling network streams or reading large files in chunks.
type StreamCallback[T any] interface {

	// OnNext is called whenever a new data item of type T is received from the stream.
	// It processes the received data item and returns an error if any issues occur during processing.
	// This method allows for custom logic to be applied to each individual data item in the stream.
	OnNext(t T) error

	// Complete is called when the stream has ended, either successfully or due to an error.
	// The 'err' parameter indicates the status of the stream completion.
	// If the stream ended successfully, 'err' will be nil. Otherwise,
	// it will contain the error that caused the stream to terminate.
	Complete(err error)
}
