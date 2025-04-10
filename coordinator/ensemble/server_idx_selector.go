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

var _ Selector = &serverIdxSelector{}

type serverIdxSelector struct {
}

func (*serverIdxSelector) SelectNew(
	candidates []model.Server,
	_ map[string]model.ServerMetadata,
	_ *policies.Policies,
	status *model.ClusterStatus,
	replicas uint32) ([]model.Server, error) {
	startIdx := status.ServerIdx
	n := len(candidates)
	res := make([]model.Server, replicas)
	for i := uint32(0); i < replicas; i++ {
		res[i] = candidates[int(startIdx+i)%n]
	}
	return res, nil
}
