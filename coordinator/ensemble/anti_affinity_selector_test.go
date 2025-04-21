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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/policies"
)

func TestGroupingCandidatesNormalCase(t *testing.T) {
	selector := &antiAffinitiesSelector{}

	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	candidates := []model.Server{server1, server2}

	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
				"type":   "compute",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-west",
				"type":   "compute",
			},
		},
	}

	result := selector.groupingCandidates(candidates, candidatesMetadata)

	assert.NotNil(t, result)

	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.Contains(t, regionGroup["us-east"], "server1")
	assert.Contains(t, regionGroup["us-west"], "server2")

	typeGroup, exists := result["type"]
	assert.True(t, exists)
	assert.Contains(t, typeGroup["compute"], "server1")
	assert.Contains(t, typeGroup["compute"], "server2")
}

func TestGroupingCandidatesNoCandidates(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	var candidates []model.Server
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := selector.groupingCandidates(candidates, candidatesMetadata)

	assert.Empty(t, result)
}

func TestGroupingCandidatesNoMetadata(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	candidates := []model.Server{server1}
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := selector.groupingCandidates(candidates, candidatesMetadata)

	assert.Empty(t, result)
}

func TestGroupingCandidatesPartialMetadataMissing(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	candidates := []model.Server{server1, server2}

	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
	}

	result := selector.groupingCandidates(candidates, candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.Contains(t, regionGroup["us-east"], "server1")
	assert.NotContains(t, regionGroup["us-east"], "server2")
}

func TestGroupingCandidatesAllSameLabelValue(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	candidates := []model.Server{server1, server2}

	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
	}

	result := selector.groupingCandidates(candidates, candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.Contains(t, regionGroup["us-east"], "server1")
	assert.Contains(t, regionGroup["us-east"], "server2")
}

func TestSelectNewNoAntiAffinities(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-west"}},
		"server3": {Labels: map[string]string{"region": "eu-east"}},
		"server4": {Labels: map[string]string{"region": "eu-west"}},
		"server5": {Labels: map[string]string{"region": "ap-east"}},
		"server6": {Labels: map[string]string{"region": "ap-west"}},
	}
	var nsPolicies *policies.Policies
	replicas := uint32(6)

	result, err := selector.SelectNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, len(candidates), len(result))
}

func TestSelectNewSatisfiedAntiAffinities(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-west"}},
		"server3": {Labels: map[string]string{"region": "eu-east"}},
		"server4": {Labels: map[string]string{"region": "eu-west"}},
		"server5": {Labels: map[string]string{"region": "ap-east"}},
		"server6": {Labels: map[string]string{"region": "ap-west"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{
				Labels:              []string{"region"},
				UnsatisfiableAction: policies.DoNotSchedule,
			},
		},
	}
	replicas := uint32(6)

	result, err := selector.SelectNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, len(candidates), len(result))
}

func TestSelectNewUnsatisfiedAntiAffinitiesDoNotSchedule(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
		"server3": {Labels: map[string]string{"region": "us-east"}},
		"server4": {Labels: map[string]string{"region": "us-east"}},
		"server5": {Labels: map[string]string{"region": "us-east"}},
		"server6": {Labels: map[string]string{"region": "us-east"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{
				Labels:              []string{"region"},
				UnsatisfiableAction: policies.DoNotSchedule,
			},
		},
	}
	replicas := uint32(6)

	result, err := selector.SelectNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsatisfied anti-affinities")
	assert.Nil(t, result)
}

func TestSelectNewUnsatisfiedAntiAffinitiesScheduleAnyway(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
		"server3": {Labels: map[string]string{"region": "us-east"}},
		"server4": {Labels: map[string]string{"region": "us-east"}},
		"server5": {Labels: map[string]string{"region": "us-east"}},
		"server6": {Labels: map[string]string{"region": "us-east"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{
				Labels:              []string{"region"},
				UnsatisfiableAction: policies.ScheduleAnyway,
			},
		},
	}
	replicas := uint32(3)

	result, err := selector.SelectNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported unsatisfiable action")
	assert.Nil(t, result)
}

func TestAllocateNewMultipleAntiAffinitiesSatisfied(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	servers := []model.Server{
		{Name: ptr.To("s1"), Public: "s1", Internal: "s1"},
		{Name: ptr.To("s2"), Public: "s2", Internal: "s2"},
		{Name: ptr.To("s3"), Public: "s3", Internal: "s3"},
		{Name: ptr.To("s4"), Public: "s4", Internal: "s4"},
	}
	candidatesMetadata := map[string]model.ServerMetadata{
		"s1": {Labels: map[string]string{"region": "us-east", "type": "compute1"}},
		"s2": {Labels: map[string]string{"region": "us-north", "type": "compute2"}},
		"s3": {Labels: map[string]string{"region": "us-south", "type": "storage1"}},
		"s4": {Labels: map[string]string{"region": "us-west", "type": "storage2"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{Labels: []string{"region"}, UnsatisfiableAction: policies.DoNotSchedule},
			{Labels: []string{"type"}, UnsatisfiableAction: policies.DoNotSchedule},
		},
	}
	replicas := uint32(4)

	result, err := selector.SelectNew(servers, candidatesMetadata, nsPolicies, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, len(servers), len(result))
}

func TestSelectNewMultipleAntiAffinitiesPartialUnsatisfied(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	servers := []model.Server{
		{Name: ptr.To("s1"), Public: "s1", Internal: "s1"},
		{Name: ptr.To("s2"), Public: "s2", Internal: "s2"},
		{Name: ptr.To("s3"), Public: "s3", Internal: "s3"},
	}
	candidatesMetadata := map[string]model.ServerMetadata{
		"s1": {Labels: map[string]string{"region": "us-east", "type": "compute"}},
		"s2": {Labels: map[string]string{"region": "us-east", "type": "compute"}},
		"s3": {Labels: map[string]string{"region": "us-east", "type": "compute"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{Labels: []string{"region"}, UnsatisfiableAction: policies.DoNotSchedule},
			{Labels: []string{"type"}, UnsatisfiableAction: policies.DoNotSchedule},
		},
	}
	replicas := uint32(3)

	result, err := selector.SelectNew(servers, candidatesMetadata, nsPolicies, nil, replicas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsatisfied anti-affinities")
	assert.Nil(t, result)
}

func TestSelectNewMixedActionPolicies(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	servers := []model.Server{
		{Name: ptr.To("s1"), Public: "s1", Internal: "s1"},
		{Name: ptr.To("s2"), Public: "s2", Internal: "s2"},
	}
	candidatesMetadata := map[string]model.ServerMetadata{
		"s1": {Labels: map[string]string{"region": "us-east", "type": "compute"}},
		"s2": {Labels: map[string]string{"region": "us-east", "type": "storage"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{Labels: []string{"region"}, UnsatisfiableAction: policies.DoNotSchedule},
			{Labels: []string{"type"}, UnsatisfiableAction: policies.ScheduleAnyway},
		},
	}
	replicas := uint32(2)

	result, err := selector.SelectNew(servers, candidatesMetadata, nsPolicies, nil, replicas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsatisfied anti-affinities")
	assert.Nil(t, result)
}

func TestSelectNewUnsupportedActionInMultiplePolicies(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	servers := []model.Server{
		{Name: ptr.To("s1"), Public: "s1", Internal: "s1"},
		{Name: ptr.To("s2"), Public: "s2", Internal: "s2"},
		{Name: ptr.To("s3"), Public: "s3", Internal: "s3"},
		{Name: ptr.To("s4"), Public: "s4", Internal: "s4"},
	}
	candidatesMetadata := map[string]model.ServerMetadata{
		"s1": {Labels: map[string]string{"region": "us-east"}},
		"s2": {Labels: map[string]string{"region": "us-east"}},
		"s3": {Labels: map[string]string{"region": "us-east"}},
		"s4": {Labels: map[string]string{"region": "us-east"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{Labels: []string{"region"}, UnsatisfiableAction: policies.ScheduleAnyway},
			{Labels: []string{"zone"}, UnsatisfiableAction: policies.DoNotSchedule},
		},
	}
	replicas := uint32(3)

	result, err := selector.SelectNew(servers, candidatesMetadata, nsPolicies, nil, replicas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported unsatisfiable action")
	assert.Nil(t, result)
}

func TestSelectNewMultiplePoliciesWithSameLabel(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	servers := []model.Server{
		{Name: ptr.To("s1"), Public: "s1", Internal: "s1"},
		{Name: ptr.To("s2"), Public: "s2", Internal: "s2"},
	}
	candidatesMetadata := map[string]model.ServerMetadata{
		"s1": {Labels: map[string]string{"region": "us-east", "rack": "rack1"}},
		"s2": {Labels: map[string]string{"region": "us-west", "rack": "rack2"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{Labels: []string{"region"}, UnsatisfiableAction: policies.DoNotSchedule},
			{Labels: []string{"rack"}, UnsatisfiableAction: policies.DoNotSchedule},
		},
	}
	replicas := uint32(2)

	result, err := selector.SelectNew(servers, candidatesMetadata, nsPolicies, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, len(servers), len(result))
}

func TestSelectNewMultiplePolicies(t *testing.T) {
	selector := &antiAffinitiesSelector{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east", "zone": "z1"}},
		"server2": {Labels: map[string]string{"region": "us-west", "zone": "z2"}},
		"server3": {Labels: map[string]string{"region": "us-north", "zone": "z3"}},
		"server4": {Labels: map[string]string{"region": "us-south", "zone": "z4"}},
		"server5": {Labels: map[string]string{"region": "us-east", "zone": "z1"}},
		"server6": {Labels: map[string]string{"region": "us-east", "zone": "z1"}},
	}

	replicas := uint32(4)

	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{Labels: []string{"region"}, UnsatisfiableAction: policies.DoNotSchedule},
			{Labels: []string{"zone"}, UnsatisfiableAction: policies.DoNotSchedule},
		},
	}

	result, err := selector.SelectNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, "server2", *result[0].Name)
	assert.Equal(t, "server3", *result[1].Name)
	assert.Equal(t, "server4", *result[2].Name)
	assert.Equal(t, "server6", *result[3].Name)
}
