// Copyright 2025 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	nsPolicies *policies.Policies,
	_ *model.ClusterStatus,
	replicas uint32) ([]model.Server, error) {
	if nsPolicies == nil {
		return candidates, nil
	}
	antiAffinities := nsPolicies.AntiAffinities
	if antiAffinities == nil || len(antiAffinities) == 0 {
		return candidates, nil
	}

	groupingCandidates := z.groupingCandidates(candidates, candidatesMetadata)

	filteredCandidates := hashset.New()
	for _, antiAffinity := range antiAffinities {
		for _, label := range antiAffinity.Labels {
			labelGroupedCandidates := groupingCandidates[label]
			for _, servers := range labelGroupedCandidates {
				if len(servers) > 1 {
					for idx := range servers[1:] {
						filteredCandidates.Add(servers[idx])
					}
					leftCandidates := len(candidates) - filteredCandidates.Size()
					if leftCandidates < int(replicas) {
						switch antiAffinity.UnsatisfiableAction {
						case policies.DoNotSchedule:
							return nil, errors.Wrap(ErrUnsatisfiedAntiAffinities, fmt.Sprintf("expectCandidates=%v actualCandidates%v", replicas, leftCandidates))
						case policies.ScheduleAnyway:
							fallthrough
						default:
							return nil, errors.Wrap(ErrUnsupportedUnsatisfiableAction, fmt.Sprintf("unsupported unsatisfiable action %v", antiAffinity.UnsatisfiableAction))
						}
					}
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
