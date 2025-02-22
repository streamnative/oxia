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

// Copyright 2025 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package callback

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
