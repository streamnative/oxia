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
