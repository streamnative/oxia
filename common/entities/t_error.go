package entities

type TWithError[T any] struct {
	T   T
	Err error
}
