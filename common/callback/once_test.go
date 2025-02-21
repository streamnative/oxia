package callback

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
)

func Test_Once_Complete_Concurrent(t *testing.T) {
	callbackCounter := atomic.Int32{}
	onceCallback := NewOnce[any](
		func(t any) {
			callbackCounter.Add(1)
		},
		func(err error) {
			callbackCounter.Add(1)
		})

	group := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		group.Add(1)
		go func() {
			if i%2 == 0 {
				onceCallback.Complete(nil)
			} else {
				onceCallback.CompleteError(errors.New("error"))
			}
			group.Done()
		}()
	}

	group.Wait()
	assert.Equal(t, int32(1), callbackCounter.Load())
}

func Test_Once_Complete(t *testing.T) {
	var callbackError error
	var callbackValue int32

	onceCallback := NewOnce[int32](
		func(t int32) {
			callbackValue = t
		},
		func(err error) {
			callbackError = err
		})

	onceCallback.Complete(1)

	assert.Nil(t, callbackError)
	assert.Equal(t, int32(1), callbackValue)
}

func Test_Once_Complete_Error(t *testing.T) {
	var callbackError error
	var callbackValue *int32

	onceCallback := NewOnce[int32](
		func(t int32) {
			callbackValue = &t
		},
		func(err error) {
			callbackError = err
		})

	e1 := errors.New("error")
	onceCallback.CompleteError(e1)

	assert.Equal(t, e1, callbackError)
	assert.Nil(t, callbackValue)
}
