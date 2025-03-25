package ensemble

import (
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/policies"
)

var (
	ErrUnsatisfiedAntiAffinities      = errors.New("unsatisfied anti-affinities")
	ErrUnsupportedUnsatisfiableAction = errors.New("unsupported unsatisfiable action")
)

type Allocator interface {
	AllocateNew(
		candidates []model.Server,
		candidatesMetadata map[string]model.ServerMetadata,
		policies *policies.Policies,
		status *model.ClusterStatus,
		replicas uint32) ([]model.Server, error)
}

func NewAllocator() Allocator {
	return &mergedAllocator{
		allocators: []Allocator{
			&antiAffinitiesAllocator{},
			&lastAllocator{},
		},
	}
}
