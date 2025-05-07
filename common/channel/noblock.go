package channel

func PushNoBlock[T any](ch chan T, t T) bool {
	select {
	case ch <- t:
		return true
	default:
		return false
	}
}
