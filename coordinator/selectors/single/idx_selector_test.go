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
	"testing"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestServerIdxSelectNew(t *testing.T) {
	selector := &serverIdxSelector{}

	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []any{server1.GetIdentifier(), server2.GetIdentifier(), server3.GetIdentifier(), server4.GetIdentifier(), server5.GetIdentifier(), server6.GetIdentifier()}

	options := &Context{
		CandidatesMetadata: make(map[string]model.ServerMetadata),
		Candidates:         linkedhashset.New(candidates...),
		Policies:           nil,
		Status: &model.ClusterStatus{
			ServerIdx: 0,
		},
	}

	for i := 0; i < 12; i++ {
		options.Status.ServerIdx = uint32(i)
		result, err := selector.Select(options)
		assert.NoError(t, err)
		assert.EqualValues(t, *result, candidates[i%6])
	}
}
