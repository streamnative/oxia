package impl

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var metadataProviders = map[string]func() MetadataProvider{
	"memory": NewMetadataProviderMemory,
}

func TestMetadataProvider(t *testing.T) {
	for name, provider := range metadataProviders {
		t.Run(name, func(t *testing.T) {
			m := provider()

			res, version, err := m.Get()
			assert.NoError(t, err)
			assert.Equal(t, MetadataNotExists, version)
			assert.Nil(t, res)

			newVersion, err := m.Store(&ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]ShardMetadata),
			}, 0)
			assert.ErrorIs(t, err, ErrorMetadataBadVersion)
			assert.Equal(t, MetadataNotExists, newVersion)

			newVersion, err = m.Store(&ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]ShardMetadata),
			}, -1)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, newVersion)

			res, version, err = m.Get()
			assert.NoError(t, err)
			assert.EqualValues(t, 0, version)
			assert.Equal(t, res, &ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]ShardMetadata),
			})

			assert.NoError(t, m.Close())
		})
	}
}
