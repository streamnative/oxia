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
	"io"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	oxia "oxia/pkg/generated/clientset/versioned"
)

type Watcher interface {
	io.Closer
}

type watcher struct {
	watch   watch.Interface
	closeCh chan bool
}

func (w *watcher) Close() error {
	w.watch.Stop()
	close(w.closeCh)
	return nil
}

func newWatcher(client oxia.Interface, reconciler Reconciler) (Watcher, error) {
	ctx := context.Background()
	_watch, err := client.OxiaV1alpha1().OxiaClusters("").Watch(ctx, v1.ListOptions{})
	if err != nil {
		return nil, err
	}
	_watcher := &watcher{
		watch:   _watch,
		closeCh: make(chan bool),
	}
	go _watcher.run(reconciler)
	return _watcher, nil
}

func (w *watcher) run(reconciler Reconciler) {
	for {
		select {
		case event := <-w.watch.ResultChan():
			reconciler.Reconcile(event)
		case <-w.closeCh:
			break
		}
	}
}
