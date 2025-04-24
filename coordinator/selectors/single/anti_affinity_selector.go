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

package single

import (
	"github.com/emirpasic/gods/sets/linkedhashset"
	p "github.com/streamnative/oxia/coordinator/policies"
	"github.com/streamnative/oxia/coordinator/selectors"
)

var _ selectors.Selector[*Context, *string] = &serverAntiAffinitiesSelector{}

type serverAntiAffinitiesSelector struct{}

func (*serverAntiAffinitiesSelector) Select(ssContext *Context) (*string, error) {
	policies := ssContext.Policies
	if policies == nil || len(policies.AntiAffinities) == 0 {
		return nil, selectors.ErrNoFunctioning
	}
	if ssContext.selected == nil {
		ssContext.selected = linkedhashset.New()
	}
	selectedLabelValues := ssContext.LabelGroupedSelectedLabelValues()
	candidates := linkedhashset.New()
	for affinityIdx, affinity := range policies.AntiAffinities {
		for _, label := range affinity.Labels {
			labelSatisfiedCandidates := linkedhashset.New()
			labelGroupedCandidates := ssContext.LabelValueGroupedCandidates()[label]
			for candidatesLabelValue, servers := range labelGroupedCandidates {
				if len(selectedLabelValues) > 0 {
					if selectedLabelValueSet, exist := selectedLabelValues[label]; exist {
						if selectedLabelValueSet.Contains(candidatesLabelValue) {
							continue
						}
					}
				}
				if result := servers.Difference(ssContext.selected); result.Size() > 0 {
					for iter := result.Iterator(); iter.Next(); {
						labelSatisfiedCandidates.Add(iter.Value())
					}
				}
			}
			if affinityIdx > 0 {
				labelSatisfiedCandidates = labelSatisfiedCandidates.Intersection(candidates)
			}
			if labelSatisfiedCandidates.Size() < 1 {
				switch affinity.Mode {
				case p.Strict:
					return nil, selectors.ErrUnsatisfiedAntiAffinity
				case p.Relaxed:
					fallthrough
				default:
					return nil, selectors.ErrUnsupportedAntiAffinityMode
				}
			}
			if affinityIdx == 0 {
				candidates.Add(labelSatisfiedCandidates.Values()...)
				continue
			}
			candidates = labelSatisfiedCandidates
		}
	}
	if candidates.Size() == 1 {
		_, value := candidates.Find(func(_ int, _ interface{}) bool { return true })
		var res = value.(string)
		return &res, nil
	}

	ssContext.Candidates = candidates
	return nil, selectors.ErrMultipleResult
}
