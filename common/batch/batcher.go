// Copyright 2023 StreamNative, Inc.
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

package batch

import (
	"errors"
	"io"
	"time"
)

var ErrorShuttingDown = errors.New("shutting down")

type Batcher interface {
	io.Closer
	Add(request any)
	Run()
}

type batcherImpl struct {
	shardId             *uint32
	batchFactory        func() Batch
	callC               chan any
	closeC              chan bool
	linger              time.Duration
	maxRequestsPerBatch int
}

func (b *batcherImpl) Close() error {
	close(b.closeC)
	return nil
}

func (b *batcherImpl) Add(call any) {
	b.callC <- call
}

func (b *batcherImpl) Run() {
	var batch Batch
	var timer *time.Timer = nil
	var timeout <-chan time.Time = nil
	for {
		select {
		case call := <-b.callC:
			if batch == nil {
				batch = b.batchFactory()
				if b.linger > 0 {
					timer = time.NewTimer(b.linger)
					timeout = timer.C
				}
			}
			batch.Add(call)
			if batch.Size() == b.maxRequestsPerBatch || b.linger == 0 {
				if b.linger > 0 {
					timer.Stop()
				}
				batch.Complete()
				batch = nil
			}
		case <-timeout:
			if batch != nil {
				timer.Stop()
				batch.Complete()
				batch = nil
			}
		case <-b.closeC:
			if batch != nil {
				timer.Stop()
				batch.Fail(ErrorShuttingDown)
				batch = nil
			}
			return
		}
	}
}
