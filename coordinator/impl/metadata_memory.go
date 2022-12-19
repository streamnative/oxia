package impl

import (
	"oxia/coordinator/model"
	"strconv"
	"sync"
)

// MetadataProviderMemory is a provider that just keeps the cluster status in memory
// Used for unit tests
type metadataProviderMemory struct {
	sync.Mutex

	cs      *model.ClusterStatus
	version Version
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

func (m *metadataProviderMemory) Get() (cs *model.ClusterStatus, version Version, err error) {
	m.Lock()
	defer m.Unlock()
	return m.cs, m.version, nil
}

func (m *metadataProviderMemory) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	m.Lock()
	defer m.Unlock()

	if expectedVersion != m.version {
		return MetadataNotExists, ErrorMetadataBadVersion
	}

	m.cs = cs.Clone()
	m.version = incrVersion(m.version)
	return m.version, nil
}

func incrVersion(version Version) Version {
	i, err := strconv.ParseInt(string(version), 10, 32)
	if err != nil {
		return ""
	}
	i++
	return Version(strconv.FormatInt(i, 10))
}
