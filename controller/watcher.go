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

package controller

import (
	"context"
	"github.com/rs/zerolog/log"
	"io"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"oxia/common"
	oxia "oxia/pkg/generated/clientset/versioned"
	"time"
)

type Watcher io.Closer

type watcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	initWg common.WaitGroup
}

func (w *watcher) Close() error {
	w.cancel()
	return nil
}

func newWatcher(client oxia.Interface, reconciler Reconciler) (Watcher, error) {
	w := &watcher{
		initWg: common.NewWaitGroup(1),
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())
	go w.runWithRetries(client, reconciler)

	// Wait until fully initialized
	if err := w.initWg.Wait(w.ctx); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *watcher) runWithRetries(client oxia.Interface, reconciler Reconciler) {
	for w.ctx.Err() == nil {
		w.run(client, reconciler)
		time.Sleep(5 * time.Second)
	}
}

func (w *watcher) run(client oxia.Interface, reconciler Reconciler) {
	watch, err := client.OxiaV1alpha1().
		OxiaClusters("").
		Watch(w.ctx, v1.ListOptions{})
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create watcher")
		return
	}

	w.initWg.Done()

	defer watch.Stop()

	for {
		select {
		case event, ok := <-watch.ResultChan():
			if !ok {
				// Watcher was already close, reopen it
				return
			}
			reconciler.Reconcile(event)
		case <-w.ctx.Done():
			return
		}
	}
}
