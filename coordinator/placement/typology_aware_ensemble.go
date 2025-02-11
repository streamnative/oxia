package placement

import (
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/model"
)

type NodeEnsemble []model.NodeInfo

type AntiAffinityEnsembleSelector interface {
	Select(currentEnsemble NodeEnsemble, availableNodes NodeEnsemble) (NodeEnsemble, error)
}

type antiAffinityEnsembleSelector struct {
	configSupplier func() *model.NamespaceConfig
}

func (t *antiAffinityEnsembleSelector) Select(currentEnsemble NodeEnsemble, availableNodes NodeEnsemble) (NodeEnsemble, error) {
	config := t.configSupplier()
	rules := config.AntiAffinityLabels

	// (1) validate if current ensemble is okay
	for _, rule := range rules {
		var ruleSet = common.NewSet[string]()
		for _, node := range currentEnsemble {
			if _, ok := node.Labels[rule]; ok {
				ruleSet.Add(node.GetID())
			}
		}
		currentEnsembleSize := len(currentEnsemble)
		if uint32(currentEnsembleSize) == config.ReplicationFactor && ruleSet.Count() == currentEnsembleSize {
			return currentEnsemble, nil
		}
		// (2) select by ensemble
		if uint32(ruleSet.Count()) < config.ReplicationFactor {
			for _, node := range availableNodes {
				if _, ok := node.Labels[rule]; ok {
					ruleSet.Add(node.GetID())
				}
			}
		}
	}
}

func NewTypologyAwareEnsembleSelector(configSupplier func() *model.NamespaceConfig) AntiAffinityEnsembleSelector {
	return &antiAffinityEnsembleSelector{}
}
