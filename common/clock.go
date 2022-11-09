package common

import "time"

type Clock interface {
	NowMillis() uint64
}

type systemClock struct {
}

func SystemClock() Clock {
	return systemClock{}
}

func (c systemClock) NowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}
