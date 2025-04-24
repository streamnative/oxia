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

package selectors

import (
	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
)

func GroupingCandidatesWithLabelValue(candidates *linkedhashset.Set, candidatesMetadata map[string]model.ServerMetadata) map[string]map[string]*linkedhashset.Set {
	groupedCandidates := make(map[string]map[string]*linkedhashset.Set)
	for iterator := candidates.Iterator(); iterator.Next(); {
		candidate := iterator.Value().(string)
		metadata, exist := candidatesMetadata[candidate]
		if !exist {
			continue
		}
		for label, labelValue := range metadata.Labels {
			labelGroup, exist := groupedCandidates[label]
			if !exist {
				tmp := make(map[string]*linkedhashset.Set)
				groupedCandidates[label] = tmp
				labelGroup = tmp
			}
			labelValueGroup, exist := labelGroup[labelValue]
			if !exist {
				tmp := linkedhashset.New()
				labelGroup[labelValue] = tmp
				labelValueGroup = tmp
			}
			labelValueGroup.Add(candidate)
		}

	}
	return groupedCandidates
}

func GroupingValueWithLabel(candidates *linkedhashset.Set, candidatesMetadata map[string]model.ServerMetadata) map[string]*linkedhashset.Set {
	selectedLabelValues := make(map[string]*linkedhashset.Set)
	for selectedIter := candidates.Iterator(); selectedIter.Next(); {
		if metadata, exist := candidatesMetadata[selectedIter.Value().(string)]; exist {
			for label, labelValue := range metadata.Labels {
				collection := selectedLabelValues[label]
				if collection == nil {
					collection = linkedhashset.New(labelValue)
				} else {
					collection.Add(labelValue)
				}
				selectedLabelValues[label] = collection
			}
		}
	}
	return selectedLabelValues
}
