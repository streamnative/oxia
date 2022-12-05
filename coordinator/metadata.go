package coordinator

import (
	"github.com/pkg/errors"
	"io"
)

var (
	ErrorMetadataNotInitialized = errors.New("metadata not initialized")
	ErrorMetadataBadVersion     = errors.New("metadata bad version")
)

const MetadataNotExists int64 = -1

type MetadataProvider interface {
	io.Closer

	Get() (cs *ClusterStatus, version int64, err error)

	Store(cs *ClusterStatus, expectedVersion int64) (newVersion int64, err error)
}
