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

package impl

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"

	"github.com/streamnative/oxia/common/metric"
	"github.com/streamnative/oxia/coordinator/model"
)

type metadataProviderConfigMap struct {
	sync.Mutex
	kubernetes      k8s.Interface
	namespace, name string

	metadataSize      atomic.Int64
	getLatencyHisto   metric.LatencyHistogram
	storeLatencyHisto metric.LatencyHistogram
	metadataSizeGauge metric.Gauge
}

func NewMetadataProviderConfigMap(kc k8s.Interface, namespace, name string) MetadataProvider {
	m := &metadataProviderConfigMap{
		kubernetes: kc,
		namespace:  namespace,
		name:       name,

		getLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_get_latency",
			"Latency for reading coordinator metadata", nil),
		storeLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_store_latency",
			"Latency for storing coordinator metadata", nil),
	}

	m.metadataSizeGauge = metric.NewGauge("oxia_coordinator_metadata_size",
		"The size of the coordinator metadata", metric.Bytes, nil, func() int64 {
			return m.metadataSize.Load()
		})

	return m
}

func (m *metadataProviderConfigMap) Get() (status *model.ClusterStatus, version Version, err error) {
	timer := m.getLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()
	return m.getWithoutLock()
}

func (m *metadataProviderConfigMap) getWithoutLock() (*model.ClusterStatus, Version, error) {
	cm, err := K8SConfigMaps(m.kubernetes).Get(m.namespace, m.name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, MetadataNotExists, nil
		}
		return nil, "", err
	}

	status := &model.ClusterStatus{}
	data := []byte(cm.Data["status"])
	if err = yaml.Unmarshal(data, status); err != nil {
		return nil, "", err
	}

	version := Version(cm.ResourceVersion)
	slog.Debug("Get metadata successful",
		slog.String("version", cm.ResourceVersion))
	m.metadataSize.Store(int64(len(data)))
	return status, version, nil
}

func (m *metadataProviderConfigMap) Store(status *model.ClusterStatus, expectedVersion Version) (Version, error) {
	timer := m.storeLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()

	_, version, err := m.getWithoutLock()
	if err != nil {
		return version, err
	}

	if version != expectedVersion {
		slog.Error("Store metadata failed for version mismatch",
			slog.Any("local-version", version),
			slog.Any("expected-version", expectedVersion))
		panic(ErrMetadataBadVersion)
	}

	data := configMap(m.name, status, expectedVersion)
	cm, err := K8SConfigMaps(m.kubernetes).Upsert(m.namespace, m.name, data)
	if k8serrors.IsConflict(err) {
		panic(err)
	}
	version = Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(data.Data["status"])))
	return version, nil
}

func (*metadataProviderConfigMap) Close() error {
	return nil
}

func configMap(name string, status *model.ClusterStatus, version Version) *corev1.ConfigMap {
	bytes, err := yaml.Marshal(status)
	if err != nil {
		slog.Error(
			"unable to marshal cluster status",
			slog.Any("error", err),
		)
		panic(err)
	}

	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Data: map[string]string{
			"status": string(bytes),
		},
	}

	if version != MetadataNotExists {
		cm.ResourceVersion = string(version)
	}

	return cm
}
