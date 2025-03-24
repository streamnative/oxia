package ensemble

import (
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/policies"
)

var _ Allocator = &mergedAllocator{}

type mergedAllocator struct {
	allocators []Allocator
}

func (l *mergedAllocator) AllocateNew(
	candidates []model.Server,
	candidatesMetadata map[string]model.ServerMetadata,
	policies *policies.Policies,
	status *model.ClusterStatus,
	replicas uint32) ([]model.Server, error) {

	leftCandidates := candidates
	var err error
	for _, allocator := range l.allocators {
		leftCandidates, err = allocator.AllocateNew(leftCandidates, candidatesMetadata, policies, status, replicas)
		if err != nil {
			return nil, err
		}
	}
	if len(leftCandidates) != int(replicas) {
		panic("unexpected number of left candidates")
	}
	return leftCandidates, nil
}
