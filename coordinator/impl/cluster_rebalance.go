// Copyright 2023 StreamNative, Inc.
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

package impl

import (
	"log/slog"
	"sort"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/model"
)

type SwapNodeAction struct {
	Shard int64
	From  model.Server
	To    model.Server
}

type ServerContext struct {
	Server model.Server
	Shards common.Set[int64]
}

// Make sure every server is assigned a similar number of shards
// Output a list of actions to be taken to rebalance the cluster.
func rebalanceCluster(servers []model.Server, currentStatus *model.ClusterStatus) []SwapNodeAction {
	res := make([]SwapNodeAction, 0)

	serversCount := len(servers)
	shardsPerServer, deletedServers := getShardsPerServer(servers, currentStatus)

outer:
	for {
		rankings := getServerRanking(shardsPerServer)
		slog.Debug("Computed rankings: ")
		for _, r := range rankings {
			slog.Debug(
				"",
				slog.Any("server", r.Server),
				slog.Int("count", r.Shards.Count()),
			)
		}
		if len(deletedServers) > 0 {
			slog.Debug("Deleted servers: ")
			for server, context := range deletedServers {
				slog.Debug(
					"",
					slog.String("server", server),
					slog.Int("count", context.Shards.Count()),
				)
			}
		}
		slog.Debug("------------------------------")

		// First try to reassign shards from the removed servers.
		// We do it one by one, by placing in the lead loaded server
		if len(deletedServers) > 0 {
			id, context := getFirstEntry(deletedServers)

			for j := serversCount - 1; j >= 0; j-- {
				to := rankings[j]
				eligibleShards := context.Shards.Complement(to.Shards)

				if !eligibleShards.IsEmpty() {
					a := SwapNodeAction{
						Shard: eligibleShards.GetSorted()[0],
						From:  context.Server,
						To:    to.Server,
					}

					context.Shards.Remove(a.Shard)
					if context.Shards.IsEmpty() {
						delete(deletedServers, id)
					} else {
						deletedServers[id] = context
					}
					shardsPerServer[a.To.GetIdentifier()].Shards.Add(a.Shard)

					slog.Debug(
						"Transfer from removed node",
						slog.Any("swap-action", a),
					)

					res = append(res, a)
					continue outer
				}
			}

			slog.Warn("It wasn't possible to reassign any shard from deleted servers")
			break
		}

		// Find a shard from the most loaded server that can be moved to the
		// least loaded server, with the constraint that multiple replicas of
		// the same shard should not be assigned to one server
		mostLoaded := rankings[0]
		leastLoaded := rankings[serversCount-1]
		if mostLoaded.Shards.Count() <= leastLoaded.Shards.Count()+1 {
			break
		}

		eligibleShards := mostLoaded.Shards.Complement(leastLoaded.Shards)
		if eligibleShards.IsEmpty() {
			break
		}

		a := SwapNodeAction{
			Shard: eligibleShards.GetSorted()[0],
			From:  mostLoaded.Server,
			To:    leastLoaded.Server,
		}

		shardsPerServer[a.From.GetIdentifier()].Shards.Remove(a.Shard)
		shardsPerServer[a.To.GetIdentifier()].Shards.Add(a.Shard)

		slog.Debug(
			"Swapping nodes",
			slog.Any("swap-action", a),
		)

		res = append(res, a)
	}

	return res
}

func getShardsPerServer(servers []model.Server, currentStatus *model.ClusterStatus) (
	existingServers map[string]ServerContext,
	deletedServers map[string]ServerContext) {
	existingServers = map[string]ServerContext{}
	deletedServers = map[string]ServerContext{}

	for _, s := range servers {
		existingServers[s.GetIdentifier()] = ServerContext{
			Server: s,
			Shards: common.NewSet[int64](),
		}
	}

	for _, nss := range currentStatus.Namespaces {
		for shardId, shard := range nss.Shards {
			for _, candidate := range shard.Ensemble {
				if _, ok := existingServers[candidate.GetIdentifier()]; ok {
					existingServers[candidate.GetIdentifier()].Shards.Add(shardId)
					continue
				}

				// This server is getting removed
				if _, ok := deletedServers[candidate.GetIdentifier()]; !ok {
					deletedServers[candidate.GetIdentifier()] = ServerContext{
						Server: candidate,
						Shards: common.NewSet[int64]()}
				}

				deletedServers[candidate.GetIdentifier()].Shards.Add(shardId)
			}
		}
	}

	return existingServers, deletedServers
}

func getServerRanking(shardsPerServer map[string]ServerContext) []ServerContext {
	res := make([]ServerContext, 0)

	for _, context := range shardsPerServer {
		res = append(res, context)
	}

	// Rank the servers from the one with most shards to the one with the least
	sort.SliceStable(res, func(i, j int) bool {
		c1 := res[i].Shards.Count()
		c2 := res[j].Shards.Count()
		if c1 != c2 {
			return c1 >= c2
		}

		// Ensure predictable sorting
		return res[i].Server.GetIdentifier() < res[j].Server.GetIdentifier()
	})
	return res
}

func getFirstEntry(m map[string]ServerContext) (string, ServerContext) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	return keys[0], m[keys[0]]
}
