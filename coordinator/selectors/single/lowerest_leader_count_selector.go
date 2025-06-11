package single

import (
	"github.com/streamnative/oxia/coordinator/selectors"
)

var _ selectors.Selector[*Context, string] = &lowerestLeaderCountSelector{}

type lowerestLeaderCountSelector struct{}

func (*lowerestLeaderCountSelector) Select(ssContext *Context) (string, error) {
	if ssContext.Candidates.Size() == 1 {
		iter := ssContext.Candidates.Iterator()
		iter.First()
		return iter.Value().(string), nil // nolint:revive
	}
	candidatesLeaderCounts := make(map[string]uint32)
	status := ssContext.Status
	for _, ns := range status.Namespaces {
		for _, shard := range ns.Shards {
			leader := shard.Leader
			leaderNodeID := leader.GetIdentifier()
			if !ssContext.Candidates.Contains(leaderNodeID) {
				continue
			}
			if count, exist := candidatesLeaderCounts[leaderNodeID]; exist {
				candidatesLeaderCounts[leaderNodeID] = count + 1
			} else {
				candidatesLeaderCounts[leaderNodeID] = 1
			}
		}
	}
	var lowerestCandidate string
	var lowerestCandidateCount uint32
	for nodeID, count := range candidatesLeaderCounts {
		if lowerestCandidate == "" {
			lowerestCandidate = nodeID
			lowerestCandidateCount = count
			continue
		}
		if count < lowerestCandidateCount {
			lowerestCandidate = nodeID
			lowerestCandidateCount = count
			continue
		}
	}
	return lowerestCandidate, nil
}
