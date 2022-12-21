package impl

import (
	"encoding/json"
	"github.com/juju/fslock"
	"github.com/pkg/errors"
	"os"
	"oxia/coordinator/model"
	"path/filepath"
)

// MetadataProviderMemory is a provider that just keeps the cluster status in a local file,
// using a lock mechanism to prevent missing updates
type metadataProviderFile struct {
	path     string
	fileLock *fslock.Lock
}

type MetadataContainer struct {
	ClusterStatus *model.ClusterStatus `json:"clusterStatus"`
	Version       Version              `json:"version"`
}

func NewMetadataProviderFile(path string) MetadataProvider {
	return &metadataProviderFile{
		path:     path,
		fileLock: fslock.New(path),
	}
}

func (m *metadataProviderFile) Close() error {
	return nil
}

func (m *metadataProviderFile) Get() (cs *model.ClusterStatus, version Version, err error) {
	content, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, MetadataNotExists, nil
		}
		return nil, MetadataNotExists, err
	}

	if len(content) == 0 {
		return nil, MetadataNotExists, nil
	}

	mc := MetadataContainer{}
	if err = json.Unmarshal(content, &mc); err != nil {
		return nil, MetadataNotExists, err
	}

	return mc.ClusterStatus, mc.Version, nil
}

func (m *metadataProviderFile) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	// Ensure directory exists
	parentDir := filepath.Dir(m.path)
	if _, err := os.Stat(parentDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(parentDir, 0755); err != nil {
				return MetadataNotExists, err
			}
		} else {
			return MetadataNotExists, err
		}
	}

	if err := m.fileLock.Lock(); err != nil {
		return "", errors.Wrap(err, "failed to acquire file lock")
	}
	defer func() { _ = m.fileLock.Unlock() }()

	_, existingVersion, err := m.Get()
	if err != nil {
		return MetadataNotExists, err
	}

	if expectedVersion != existingVersion {
		return MetadataNotExists, ErrorMetadataBadVersion
	}

	newVersion = incrVersion(existingVersion)
	newContent, err := json.Marshal(MetadataContainer{
		ClusterStatus: cs,
		Version:       newVersion,
	})
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(m.path, newContent, 0640); err != nil {
		return MetadataNotExists, err
	}

	return newVersion, nil
}
