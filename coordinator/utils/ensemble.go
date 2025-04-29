package utils

import (
	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
)

func FilterEnsemble(ensemble []model.Server, filterNodeId string) *linkedhashset.Set {
	selected := linkedhashset.New()
	for _, candidate := range ensemble {
		nodeID := candidate.GetIdentifier()
		if nodeID == filterNodeId {
			continue
		}
		selected.Add(candidate.GetIdentifier())
	}
	return selected
}
