package impl

import (
	"github.com/pkg/errors"
	"io"
	"oxia/coordinator/model"
)

type Version string

var (
	ErrorMetadataNotInitialized = errors.New("metadata not initialized")
	ErrorMetadataBadVersion     = errors.New("metadata bad version")
)

const MetadataNotExists Version = "-1"

type MetadataProvider interface {
	io.Closer

	Get() (cs *model.ClusterStatus, version Version, err error)

	Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error)
}
