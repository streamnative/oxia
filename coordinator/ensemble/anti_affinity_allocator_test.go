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

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/policies"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestGroupingCandidates_NormalCase(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}

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

	result := allocator.groupingCandidates(candidates, candidatesMetadata)

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

func TestGroupingCandidates_NoCandidates(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	var candidates []model.Server
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := allocator.groupingCandidates(candidates, candidatesMetadata)

	assert.Empty(t, result)
}

func TestGroupingCandidates_NoMetadata(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	candidates := []model.Server{server1}
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := allocator.groupingCandidates(candidates, candidatesMetadata)

	assert.Empty(t, result)
}

func TestGroupingCandidates_PartialMetadataMissing(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	candidates := []model.Server{server1, server2}

	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
	}

	result := allocator.groupingCandidates(candidates, candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.Contains(t, regionGroup["us-east"], "server1")
	assert.NotContains(t, regionGroup["us-east"], "server2")
}

func TestGroupingCandidates_AllSameLabelValue(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	candidates := []model.Server{server1, server2}

	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
	}

	result := allocator.groupingCandidates(candidates, candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.Contains(t, regionGroup["us-east"], "server1")
	assert.Contains(t, regionGroup["us-east"], "server2")
}

func TestAllocateNew_NoAntiAffinities(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-west",
			},
		},
		"server3": {
			Labels: map[string]string{
				"region": "eu-east",
			},
		},
		"server4": {
			Labels: map[string]string{
				"region": "eu-west",
			},
		},
		"server5": {
			Labels: map[string]string{
				"region": "ap-east",
			},
		},
		"server6": {
			Labels: map[string]string{
				"region": "ap-west",
			},
		},
	}
	var nsPolicies *policies.Policies
	replicas := uint32(6)

	result, err := allocator.AllocateNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, len(candidates), len(result))
}

func TestAllocateNew_SatisfiedAntiAffinities(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-west",
			},
		},
		"server3": {
			Labels: map[string]string{
				"region": "eu-east",
			},
		},
		"server4": {
			Labels: map[string]string{
				"region": "eu-west",
			},
		},
		"server5": {
			Labels: map[string]string{
				"region": "ap-east",
			},
		},
		"server6": {
			Labels: map[string]string{
				"region": "ap-west",
			},
		},
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

	result, err := allocator.AllocateNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, len(candidates), len(result))
}

func TestAllocateNew_UnsatisfiedAntiAffinities_DoNotSchedule(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server3": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server4": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server5": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server6": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
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

	result, err := allocator.AllocateNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsatisfied anti-affinities")
	assert.Nil(t, result)
}

func TestAllocateNew_UnsatisfiedAntiAffinities_ScheduleAnyway(t *testing.T) {
	allocator := &antiAffinitiesAllocator{}
	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server3": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server4": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server5": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
		"server6": {
			Labels: map[string]string{
				"region": "us-east",
			},
		},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{
				Labels:              []string{"region"},
				UnsatisfiableAction: policies.ScheduleAnyway,
			},
		},
	}
	replicas := uint32(6)

	result, err := allocator.AllocateNew(candidates, candidatesMetadata, nsPolicies, nil, replicas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported unsatisfiable action")
	assert.Nil(t, result)
}
