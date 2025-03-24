package ensemble

import (
	"fmt"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/policies"
)

var _ Allocator = &antiAffinitiesAllocator{}

type antiAffinitiesAllocator struct {
}

func (z *antiAffinitiesAllocator) AllocateNew(
	candidates []model.Server,
	candidatesMetadata map[string]model.ServerMetadata,
	policies *policies.Policies,
	_ *model.ClusterStatus,
	replicas uint32) ([]model.Server, error) {
	antiAffinities := policies.AntiAffinities
	if antiAffinities == nil || len(antiAffinities) == 0 {
		return candidates, nil
	}

	groupingCandidates := z.groupingCandidates(candidates, candidatesMetadata)

	var filteredCandidates hashset.Set
	for _, antiAffinity := range policies.AntiAffinities {
		for _, label := range antiAffinity.Labels {
			labelGroupedCandidates := groupingCandidates[label]
			for _, servers := range labelGroupedCandidates {
				if len(servers) > 1 {
					filteredCandidates.Add(servers[1:])
				}
			}
		}
	}
	var leftCandidates []model.Server
	for _, candidate := range candidates {
		if filteredCandidates.Contains(candidate.GetIdentifier()) {
			continue
		}
		leftCandidates = append(leftCandidates, candidate)
	}
	if len(leftCandidates) < int(replicas) {
		return nil, errors.Wrap(ErrUnsatisfiedAntiAffinities, fmt.Sprintf("expectCandidates=%v actualCandidates%v", replicas, len(leftCandidates)))
	}
	return leftCandidates, nil
}

func (z *antiAffinitiesAllocator) groupingCandidates(candidates []model.Server, candidatesMetadata map[string]model.ServerMetadata) map[string]map[string][]string {
	groupedCandidates := make(map[string]map[string][]string)
	for idx, candidate := range candidates {
		id := candidate.GetIdentifier()
		metadata, exist := candidatesMetadata[id]
		if !exist {
			continue
		}
		for label, labelValue := range metadata.Labels {
			labelGroup, exist := groupedCandidates[label]
			if !exist {
				tmp := make(map[string][]string)
				groupedCandidates[label] = tmp
				labelGroup = tmp
			}
			_, exist = labelGroup[labelValue]
			if !exist {
				tmp := make([]string, 0)
				labelGroup[labelValue] = tmp
			}
			labelGroup[labelValue] = append(labelGroup[labelValue], candidates[idx].GetIdentifier())
		}
	}
	return groupedCandidates
}
