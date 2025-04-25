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

package callback

type streamCallbackChannelAdaptor[T any] struct {
	dataCh chan T
	errCh  chan error
}

func (c *streamCallbackChannelAdaptor[T]) OnNext(t T) error {
	c.dataCh <- t
	return nil
}

func (c *streamCallbackChannelAdaptor[T]) OnComplete(err error) {
	if err != nil {
		c.errCh <- err
	}
	close(c.dataCh)
	close(c.errCh)
}

func ReadFromStreamCallback[T any](dataCh chan T, errCh chan error) StreamCallback[T] {
	adaptor := &streamCallbackChannelAdaptor[T]{dataCh: dataCh, errCh: errCh}
	return NewStreamOnce(adaptor)
}

type streamCallbackCompleteOnly struct{ onComplete func(err error) }

func (c *streamCallbackCompleteOnly) OnNext(any) error     { return nil }
func (c *streamCallbackCompleteOnly) OnComplete(err error) { c.onComplete(err) }
