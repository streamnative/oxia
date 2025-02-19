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
