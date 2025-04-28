package entities

import "time"

type TWithTimestamp[T any] struct {
	T         T
	Timestamp time.Time
}
