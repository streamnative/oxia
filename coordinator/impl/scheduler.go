package impl

import (
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/coordinator/model"
	"math/rand"
)

type Scheduler interface {
	SelectNewEnsemble(
		candidates []model.Server,
		candidatesMetadata map[string]model.ServerMetadata,
		num int,
	) []model.Server
}

type scheduler struct {
	policiesSupplier      func() *model.HierarchyPolicies
	clusterStatusSupplier func() *model.ClusterStatus
}

func (es *scheduler) SelectNewEnsemble(
	candidates []model.Server,
	candidatesMetadata map[string]model.ServerMetadata,
	num int,
) ([]model.Server, error) {
	satisfiable, server, restServer, err := es.selectByAntiAffinity(candidates, candidatesMetadata, num)
	if err != nil {
		return nil, err
	}
	if len(server) > num || !satisfiable {
		if !satisfiable {
			return append(server, es.selectLowerShardsNum(restServer, candidatesMetadata, num)...), nil
		}
		return es.selectLowerShardsNum(server, candidatesMetadata, num), nil
	}
	return server, nil
}

func (es *scheduler) selectLowerShardsNum(candidates []model.Server, candidatesMetadata map[string]model.ServerMetadata, num int) []model.Server {
	clusterStatus := es.clusterStatusSupplier()
	namespaces := clusterStatus.Namespaces
	for _, v := range namespaces {
		for _, shard := range v.Shards {
			ensembles := shard.Ensemble
		}
	}
}

func (es *scheduler) selectByAntiAffinity(candidates []model.Server, candidatesMetadata map[string]model.ServerMetadata, num int) (
	satisfiable bool, satisfiableServer []model.Server, restServer []model.Server, err error) {
	var filteredCandidates []model.Server
	var restCandidates []model.Server
	policies := es.policiesSupplier()
	if policies != nil {
		antiAffinity := policies.AntiAffinity
		// label filter
		deduplicateMap := treemap.NewWithStringComparator()
		for _, label := range antiAffinity.Labels {
			set, found := deduplicateMap.Get(label)
			if !found {
				set = hashset.New()
				deduplicateMap.Put(label, &set)
			}
			hashSet := set.(*hashset.Set)
			for idx, server := range candidates {
				serverIdentifier := server.GetIdentifier()
				metadata, exist := candidatesMetadata[serverIdentifier]
				if !exist {
					continue
				}
				serverLabelValue, exist := metadata.Labels[label]
				if !exist {
					continue
				}
				if !hashSet.Contains(serverLabelValue) {
					hashSet.Add(serverLabelValue)
					filteredCandidates = append(filteredCandidates, candidates[idx])
					continue
				}
				restCandidates = append(restCandidates, candidates[idx])
			}
		}
		var filteredCandidatesNum = len(filteredCandidates)
		if filteredCandidatesNum >= num {
			return true, filteredCandidates, nil, nil
		} else {
			switch antiAffinity.WhenUnsatisfiable {
			case model.DoNotSchedule:
				return false, nil, nil, errors.New("unsatisfiable constraint action")
			case model.ScheduleAnyway:
				restSelect := RandomSelect(restCandidates, filteredCandidatesNum-num)
				filteredCandidates = append(filteredCandidates, restSelect...)
				return false, filteredCandidates, nil, nil
			}
		}
	}
}

func RandomSelect[T any](arr []T, num int) []T {
	if num > len(arr) {
		panic(fmt.Sprintf("num %d is longer than arr length %d", num, len(arr)))
	}
	for i := 0; i < num; i++ {
		j := rand.Intn(len(arr)-i) + i
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr[:num]
}

// 5 servers (2 exist) =>

// => select(servers, serverMetadata, shardMetadata, number)
// 1. label filters
// 2. policies filter
// 3. choose by number
