package impl

import (
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
	coreV1 "k8s.io/api/core/v1"
	k8sError "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"oxia/coordinator/model"
	"oxia/kubernetes"
	"sync"
)

type metadataProviderConfigMap struct {
	sync.Mutex
	kubernetes      k8s.Interface
	namespace, name string
}

func NewMetadataProviderConfigMap(namespace, name string) MetadataProvider {
	config := kubernetes.NewClientConfig()
	return &metadataProviderConfigMap{
		kubernetes: kubernetes.NewKubernetesClientset(config),
		namespace:  namespace,
		name:       name,
	}
}

func (m *metadataProviderConfigMap) Get() (status *model.ClusterStatus, version Version, err error) {
	m.Lock()
	defer m.Unlock()
	return m.getWithoutLock()
}

func (m *metadataProviderConfigMap) getWithoutLock() (status *model.ClusterStatus, version Version, err error) {
	cm, err := kubernetes.ConfigMaps(m.kubernetes).Get(m.namespace, m.name)
	if err != nil {
		if k8sError.IsNotFound(err) {
			err = nil
			version = MetadataNotExists
		}
		return
	}

	status = &model.ClusterStatus{}
	if err = yaml.Unmarshal([]byte(cm.Data["status"]), status); err != nil {
		return
	}
	version = Version(cm.ResourceVersion)
	return
}

func (m *metadataProviderConfigMap) Store(status *model.ClusterStatus, expectedVersion Version) (version Version, err error) {
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

	cm, err := kubernetes.ConfigMaps(m.kubernetes).Upsert(m.namespace, configMap(m.name, status, expectedVersion))
	if k8sError.IsConflict(err) {
		err = ErrorMetadataBadVersion
	}
	version = Version(cm.ResourceVersion)
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
