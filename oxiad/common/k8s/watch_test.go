// Copyright 2023-2026 The Oxia Authors
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

package k8s

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const waitTimeout = 10 * time.Second

func TestWatchConfigMapSignalsChanges(t *testing.T) {
	for _, eventType := range []k8swatch.EventType{k8swatch.Added, k8swatch.Modified, k8swatch.Deleted} {
		t.Run(string(eventType), func(t *testing.T) {
			kc, fw := newFakeWatchClient()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			changes, err := WatchConfigMap(ctx, kc, "ns", "config")
			require.NoError(t, err)

			fw.Action(eventType, newConfigMap())
			requireSignal(t, changes)
		})
	}
}

func TestWatchConfigMapIgnoresBookmark(t *testing.T) {
	kc, fw := newFakeWatchClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changes, err := WatchConfigMap(ctx, kc, "ns", "config")
	require.NoError(t, err)

	// The fake watch is unbuffered: the second send returns only once the first
	// bookmark was fully handled
	fw.Action(k8swatch.Bookmark, newConfigMap())
	fw.Action(k8swatch.Bookmark, newConfigMap())
	select {
	case <-changes:
		assert.Fail(t, "the bookmark was signalled as a change")
	default:
	}
}

func TestWatchConfigMapClosesOnErrorEvent(t *testing.T) {
	kc, fw := newFakeWatchClient()

	changes, err := WatchConfigMap(context.Background(), kc, "ns", "config")
	require.NoError(t, err)

	fw.Error(&metav1.Status{Status: metav1.StatusFailure})
	requireClosed(t, changes)
	assert.True(t, fw.IsStopped())
}

func TestWatchConfigMapClosesWhenWatchEnds(t *testing.T) {
	kc, fw := newFakeWatchClient()

	changes, err := WatchConfigMap(context.Background(), kc, "ns", "config")
	require.NoError(t, err)

	fw.Stop()
	requireClosed(t, changes)
}

func TestWatchConfigMapClosesOnCancel(t *testing.T) {
	kc, fw := newFakeWatchClient()
	ctx, cancel := context.WithCancel(context.Background())

	changes, err := WatchConfigMap(ctx, kc, "ns", "config")
	require.NoError(t, err)

	cancel()
	requireClosed(t, changes)
	assert.True(t, fw.IsStopped())
}

func TestWatchConfigMapReturnsWatchError(t *testing.T) {
	kc := fake.NewClientset()
	watchErr := errors.New("watch failed")
	kc.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, k8swatch.Interface, error) {
		return true, nil, watchErr
	})

	changes, err := WatchConfigMap(context.Background(), kc, "ns", "config")

	assert.ErrorIs(t, err, watchErr)
	assert.Nil(t, changes)
}

func newFakeWatchClient() (*fake.Clientset, *k8swatch.FakeWatcher) {
	kc := fake.NewClientset()
	fw := k8swatch.NewFake()
	kc.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, k8swatch.Interface, error) {
		return true, fw, nil
	})
	return kc, fw
}

func newConfigMap() runtime.Object {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "ns"}}
}

func requireSignal(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-changes:
		require.True(t, ok, "the channel closed instead of signalling a change")
	case <-time.After(waitTimeout):
		require.FailNow(t, "no change signalled")
	}
}

func requireClosed(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-changes:
			return !ok
		default:
			return false
		}
	}, waitTimeout, 10*time.Millisecond)
}
