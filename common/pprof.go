package common

import (
	"context"
	"runtime/pprof"
)

// DoWithLabels attaches the labels to the current go-routine Pprof context,
// for the duration of the call to f
func DoWithLabels(labels map[string]string, f func()) {
	var l []string
	for k, v := range labels {
		l = append(l, k)
		l = append(l, v)
	}

	pprof.Do(
		context.Background(),
		pprof.Labels(l...),
		func(_ context.Context) {
			f()
		})
}
