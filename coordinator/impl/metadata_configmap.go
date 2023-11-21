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
	"github.com/rs/zerolog/log"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/coordinator/model"
	"gopkg.in/yaml.v2"
	coreV1 "k8s.io/api/core/v1"
	k8sError "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"sync"
	"sync/atomic"
)

type metadataProviderConfigMap struct {
	sync.Mutex
	kubernetes      k8s.Interface
	namespace, name string

	metadataSize      atomic.Int64
	getLatencyHisto   metrics.LatencyHistogram
	storeLatencyHisto metrics.LatencyHistogram
	metadataSizeGauge metrics.Gauge
}

func NewMetadataProviderConfigMap(k8s k8s.Interface, namespace, name string) MetadataProvider {
	m := &metadataProviderConfigMap{
		kubernetes: k8s,
		namespace:  namespace,
		name:       name,

		getLatencyHisto: metrics.NewLatencyHistogram("oxia_coordinator_metadata_get_latency",
			"Latency for reading coordinator metadata", nil),
		storeLatencyHisto: metrics.NewLatencyHistogram("oxia_coordinator_metadata_store_latency",
			"Latency for storing coordinator metadata", nil),
	}

	m.metadataSizeGauge = metrics.NewGauge("oxia_coordinator_metadata_size",
		"The size of the coordinator metadata", metrics.Bytes, nil, func() int64 {
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

func (m *metadataProviderConfigMap) getWithoutLock() (status *model.ClusterStatus, version Version, err error) {
	cm, err := K8SConfigMaps(m.kubernetes).Get(m.namespace, m.name)
	if err != nil {
		if k8sError.IsNotFound(err) {
			err = nil
			version = MetadataNotExists
		}
		return
	}

	status = &model.ClusterStatus{}
	data := []byte(cm.Data["status"])
	if err = yaml.Unmarshal(data, status); err != nil {
		return
	}
	version = Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(data)))
	return
}

func (m *metadataProviderConfigMap) Store(status *model.ClusterStatus, expectedVersion Version) (version Version, err error) {
	timer := m.storeLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()

	_, version, err = m.getWithoutLock()
	if err != nil {
		return
	}

	if version != expectedVersion {
		err = ErrorMetadataBadVersion
		return
	}

	data := configMap(m.name, status, expectedVersion)
	cm, err := K8SConfigMaps(m.kubernetes).Upsert(m.namespace, data)
	if k8sError.IsConflict(err) {
		err = ErrorMetadataBadVersion
	}
	version = Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(data.Data["status"])))
	return
}

func (m *metadataProviderConfigMap) Close() error {
	return nil
}

func configMap(name string, status *model.ClusterStatus, version Version) *coreV1.ConfigMap {
	bytes, err := yaml.Marshal(status)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to marshal cluster status")
	}

	cm := &coreV1.ConfigMap{
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
