package utils

import (
	"testing"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/stretchr/testify/assert"
)

func TestGroupingCandidatesNormalCase(t *testing.T) {
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

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)

	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.True(t, regionGroup["us-west"].Contains("server2"))

	typeGroup, exists := result["type"]
	assert.True(t, exists)
	assert.True(t, typeGroup["compute"].Contains("server1"))
	assert.True(t, typeGroup["compute"].Contains("server2"))
}

func TestGroupingCandidatesNoCandidates(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New(), candidatesMetadata)
	assert.Empty(t, result)
}

func TestGroupingCandidatesNoMetadata(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.Empty(t, result)
}

func TestGroupingCandidatesPartialMetadataMissing(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
	}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.False(t, regionGroup["us-east"].Contains("server2"))
}

func TestGroupingCandidatesAllSameLabelValue(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
	}
	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.True(t, regionGroup["us-east"].Contains("server2"))
}
