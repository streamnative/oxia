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
	"sync"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
	p "github.com/streamnative/oxia/coordinator/policies"
	"github.com/streamnative/oxia/coordinator/selectors"
)

type Context struct {
	Candidates         *linkedhashset.Set
	CandidatesMetadata map[string]model.ServerMetadata
	Policies           *p.Policies
	Status             *model.ClusterStatus

	selected *linkedhashset.Set

	candidateOnce               sync.Once
	labelValueGroupedCandidates map[string]map[string]*linkedhashset.Set

	selectedOnce                    sync.Once
	labelGroupedSelectedLabelValues map[string]*linkedhashset.Set
}

func (so *Context) LabelValueGroupedCandidates() map[string]map[string]*linkedhashset.Set {
	so.maybeGrouping()
	return so.labelValueGroupedCandidates
}

func (so *Context) LabelGroupedSelectedLabelValues() map[string]*linkedhashset.Set {
	so.maybeGrouping()
	return so.labelGroupedSelectedLabelValues
}

func (so *Context) SetSelected(selected *linkedhashset.Set) {
	so.selected = selected
	so.selectedOnce = sync.Once{}
}

func (so *Context) maybeGrouping() {
	so.candidateOnce.Do(func() {
		so.labelValueGroupedCandidates = selectors.GroupingCandidatesWithLabelValue(so.Candidates, so.CandidatesMetadata)
	})
	so.selectedOnce.Do(func() {
		so.labelGroupedSelectedLabelValues = selectors.GroupingValueWithLabel(so.selected, so.CandidatesMetadata)
	})
}
