package ensemble

import (
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/policies"
)

var _ Allocator = &lastAllocator{}

type lastAllocator struct {
}

func (l lastAllocator) AllocateNew(
	candidates []model.Server,
	_ map[string]model.ServerMetadata,
	_ *policies.Policies,
	_ *model.ClusterStatus,
	replicas uint32) ([]model.Server, error) {
	return candidates[0:replicas], nil
}
