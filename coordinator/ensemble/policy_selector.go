package ensemble

import (
	"github.com/streamnative/oxia/coordinator/model"
	"sync"
)

type PolicySelector struct {
	nodes sync.Map

	antiAffinityPolicy model.AntiAffinityPolicy
}

func (pl *PolicySelector) Select() []model.NodeInfo {
	antiLabels := pl.antiAffinityPolicy.Labels

	candidates := make([]*model.NodeInfo, 0)
	pl.nodes.Range(func(key, value any) bool {
		currentNode := value.(*model.NodeInfo)
	nodeLoop:
		for _, candidate := range candidates {
			for _, antiLabel := range antiLabels {
				label, ok := candidate.Labels[antiLabel]
				if !ok {
					break nodeLoop
				}

			}
		}
		candidates = append(candidates, info)
	})
}
