package impl

import (
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/fake"
	"oxia/coordinator/model"
	k8sTesting "oxia/kubernetes/testing"
	"testing"
)

var (
	_fake = func() *fake.Clientset {
		f := fake.NewSimpleClientset()
		f.PrependReactor("*", "*", k8sTesting.ResourceVersionSupport(f.Tracker()))
		return f
	}()
	metadataProviders = map[string]func() MetadataProvider{
		"memory": NewMetadataProviderMemory,
		"configmap": func() MetadataProvider {
			return &metadataProviderConfigMap{
				kubernetes: _fake,
				namespace:  "ns",
				name:       "n",
			}
		},
	}
)

func TestMetadataProvider(t *testing.T) {
	for name, provider := range metadataProviders {
		t.Run(name, func(t *testing.T) {
			m := provider()

			res, version, err := m.Get()
			assert.NoError(t, err)
			assert.Equal(t, MetadataNotExists, version)
			assert.Nil(t, res)

			newVersion, err := m.Store(&model.ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]model.ShardMetadata),
			}, "")
			assert.ErrorIs(t, err, ErrorMetadataBadVersion)
			assert.Equal(t, MetadataNotExists, newVersion)

			newVersion, err = m.Store(&model.ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]model.ShardMetadata),
			}, MetadataNotExists)
			assert.NoError(t, err)
			assert.EqualValues(t, Version("0"), newVersion)

			res, version, err = m.Get()
			assert.NoError(t, err)
			assert.EqualValues(t, Version("0"), version)
			assert.Equal(t, &model.ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]model.ShardMetadata),
			}, res)

			assert.NoError(t, m.Close())
		})
	}
}
