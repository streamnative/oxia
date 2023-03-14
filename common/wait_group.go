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

package common

import (
	"context"
	"sync"
)

// WaitGroup is similar to sync.WaitGroup but adds 2 capabilities:
//  1. Returning an error if any operation fails
//  2. Accept a context to cancel the Wait
type WaitGroup interface {

	// Wait until all the parties in the group are either done or if there is any failure
	// You should only call wait once
	Wait(ctx context.Context) error

	// Add adds delta, which may be negative, to the WaitGroup counter.
	Add(delta int)

	// Done Signals that one party in the group is done
	Done()

	// Fail Signal that one party has failed in the operation
	Fail(err error)
}

type waitGroup struct {
	sync.WaitGroup
	errCh chan error
}

func NewWaitGroup(initial int) WaitGroup {
	wg := waitGroup{
		errCh: make(chan error, 1),
	}
	wg.Add(initial)
	return &wg
}

func (g *waitGroup) Wait(ctx context.Context) error {
	waitCh := make(chan any)
	go func() {
		defer close(waitCh)
		g.WaitGroup.Wait()
	}()
	select {
	case <-waitCh:
		return nil
	case err := <-g.errCh:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (g *waitGroup) Fail(err error) {
	g.errCh <- err
}
