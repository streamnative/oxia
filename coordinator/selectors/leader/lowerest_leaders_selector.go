package leader

import (
	"math/rand"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/selectors"
	"github.com/oxia-db/oxia/coordinator/utils"
)

var _ selectors.Selector[*Context, model.Server] = &leader{}

type leader struct{}

func (l *leader) Select(context *Context) (model.Server, error) {
	status := context.Status
	candidates := linkedhashset.New[string]()
	for _, candidate := range context.Candidates {
		candidates.Add(candidate.GetIdentifier())
	}
	_, _, leaders := utils.NodeShardLeaders(candidates, status)

	minLeaders := -1
	var minLeadersNode model.Server

	for idx, candidate := range context.Candidates {
		if shards, exist := leaders[candidate.GetIdentifier()]; exist {
			leaderNum := len(shards)
			if minLeaders == -1 || leaderNum < minLeaders {
				minLeaders = leaderNum
				minLeadersNode = context.Candidates[idx]
			}
		}
	}
	if minLeaders == -1 {
		return context.Candidates[rand.Intn(len(context.Candidates))], nil
	}
	return minLeadersNode, nil
}

func NewSelector() selectors.Selector[*Context, model.Server] {
	return &leader{}
}
