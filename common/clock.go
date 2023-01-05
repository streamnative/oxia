package common

import "time"

type Clock interface {
	Now() time.Time
}

type systemClock struct {
}

var SystemClock = &systemClock{}

func (systemClock) Now() time.Time {
	return time.Now()
}
