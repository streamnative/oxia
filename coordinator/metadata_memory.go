package coordinator

import "sync"

// MetadataProviderMemory is a provider that just keeps the cluster status in memory
// Used for unit tests
type metadataProviderMemory struct {
	sync.Mutex

	cs      *ClusterStatus
	version int64
}

func NewMetadataProviderMemory() MetadataProvider {
	return &metadataProviderMemory{
		cs:      nil,
		version: MetadataNotExists,
	}
}

func (m *metadataProviderMemory) Close() error {
	return nil
}

func (m *metadataProviderMemory) Get() (cs *ClusterStatus, version int64, err error) {
	m.Lock()
	defer m.Unlock()
	return m.cs, m.version, nil
}

func (m *metadataProviderMemory) Store(cs *ClusterStatus, expectedVersion int64) (newVersion int64, err error) {
	m.Lock()
	defer m.Unlock()

	if expectedVersion != m.version {
		return MetadataNotExists, ErrorMetadataBadVersion
	}

	m.cs = cs.Clone()
	m.version++
	return m.version, nil
}
