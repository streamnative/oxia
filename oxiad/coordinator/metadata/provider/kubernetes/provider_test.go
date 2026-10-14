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

package kubernetes

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	metadataprovider "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

func TestProviderUsesConfiguredCoordinatorName(t *testing.T) {
	p, err := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	provider := p.(*Provider[*commonproto.ClusterStatus])
	require.Equal(t, "coordinator-0", provider.name)
}

func TestProviderRejectsEmptyCoordinatorName(t *testing.T) {
	_, err := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		" ",
	)
	require.ErrorContains(t, err, "coordinator name must not be empty")
}

func TestProviderGetLeaderNameUsesLeaderElector(t *testing.T) {
	kc := fake.NewSimpleClientset()

	p, err := NewProvider(
		t.Context(),
		kc,
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err = p.WaitToBecomeLeader()
	require.NoError(t, err)

	leader, err := p.GetLeaderName()
	require.NoError(t, err)
	require.Equal(t, "coordinator-0", leader)
}

func TestProviderGetLeaderNameUnavailable(t *testing.T) {
	p, err := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err = p.GetLeaderName()
	require.True(t, errors.Is(err, metadataprovider.ErrCoordinatorLeaderUnavailable))
}

// The operators manage the config map of the configuration, e.g. with Helm.
// kubectl replace --force, kubectl delete followed by kubectl apply, and a Helm
// replace delete it before they create it again, and an edit can remove its
// key. A reload in between waits for the document to be restored: an empty
// configuration would drain every data server and delete every namespace.
func TestProviderReloadKeepsLoadedDocumentUntilConfigMapIsRestored(t *testing.T) {
	kc := fake.NewSimpleClientset()
	configMaps := kc.CoreV1().ConfigMaps("ns")
	_, err := configMaps.Create(t.Context(), newConfigMap(t, 1), metav1.CreateOptions{})
	require.NoError(t, err)
	p := newConfigProvider(t, kc)
	requireServers(t, p, 1)

	require.NoError(t, configMaps.Delete(t.Context(), "config", metav1.DeleteOptions{}))
	recreated := newConfigMap(t, 2)
	restoreLater(t, func() error {
		_, err := configMaps.Create(t.Context(), recreated, metav1.CreateOptions{})
		return err
	})
	require.NoError(t, p.Reload())
	requireServers(t, p, 2)

	_, err = configMaps.Update(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "config"}},
		metav1.UpdateOptions{})
	require.NoError(t, err)
	restored := newConfigMap(t, 3)
	restoreLater(t, func() error {
		_, err := configMaps.Update(t.Context(), restored, metav1.UpdateOptions{})
		return err
	})
	require.NoError(t, p.Reload())
	requireServers(t, p, 3)
}

// A config map that the coordinator created, e.g. from its initial cluster
// configuration, is kept in the same way.
func TestProviderReloadKeepsStoredDocumentUntilConfigMapIsRestored(t *testing.T) {
	kc := fake.NewSimpleClientset()
	p := newConfigProvider(t, kc)
	_, err := p.Store(metadataprovider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   newConfig(1),
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	configMaps := kc.CoreV1().ConfigMaps("ns")
	require.NoError(t, configMaps.Delete(t.Context(), "config", metav1.DeleteOptions{}))
	recreated := newConfigMap(t, 2)
	restoreLater(t, func() error {
		_, err := configMaps.Create(t.Context(), recreated, metav1.CreateOptions{})
		return err
	})
	require.NoError(t, p.Reload())
	requireServers(t, p, 2)
}

func newConfigProvider(t *testing.T, kc *fake.Clientset) metadataprovider.Provider[*commonproto.ClusterConfiguration] {
	t.Helper()

	p, err := NewProvider(
		t.Context(),
		kc,
		"ns",
		"config",
		metadatacodec.ClusterConfigCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})
	return p
}

// restoreLater calls restore from another goroutine, after a delay: a reload
// that the caller starts right away first finds the config map as the caller
// left it, deleted or without its key.
func restoreLater(t *testing.T, restore func() error) {
	t.Helper()

	restored := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		restored <- restore()
	}()
	t.Cleanup(func() {
		assert.NoError(t, <-restored)
	})
}

// newConfigMap returns the config map of a configuration with the given number
// of servers.
func newConfigMap(t *testing.T, servers int) *corev1.ConfigMap {
	t.Helper()

	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(newConfig(servers))
	require.NoError(t, err)
	return makeDesiredConfigMap("config", metadatacodec.ClusterConfigCodec.GetKey(), data, metadatacommon.NotExists)
}

func newConfig(servers int) *commonproto.ClusterConfiguration {
	config := &commonproto.ClusterConfiguration{}
	for i := range servers {
		config.Servers = append(config.Servers, &commonproto.DataServerIdentity{
			Public:   fmt.Sprintf("server-%d:6648", i),
			Internal: fmt.Sprintf("server-%d:6649", i),
		})
	}
	return config
}

func requireServers(t *testing.T, p metadataprovider.Provider[*commonproto.ClusterConfiguration], servers int) {
	t.Helper()

	loaded, err := p.Load()
	require.NoError(t, err)
	require.Len(t, loaded.Value.GetServers(), servers)
}
