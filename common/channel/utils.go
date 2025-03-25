package channel

// Poll tries to receive a value from the channel without blocking.
// It returns the value and a boolean indicating whether a value was received.
// If the channel has no data, it returns the zero value of the type and false.
func Poll[T any](ch <-chan T) (T, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		var n T
		return n, false
	}
}
