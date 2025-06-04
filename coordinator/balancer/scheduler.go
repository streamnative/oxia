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

package balancer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/common/process"

	"github.com/streamnative/oxia/common/channel"
	"github.com/streamnative/oxia/common/entities"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/selectors"
	"github.com/streamnative/oxia/coordinator/selectors/single"
	"github.com/streamnative/oxia/coordinator/utils"
)

var _ LoadBalancer = &nodeBasedBalancer{}

type nodeBasedBalancer struct {
	*slog.Logger
	*sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	actionCh chan Action

	statusSupplier            func() *model.ClusterStatus
	namespaceConfigSupplier   func(namespace string) *model.NamespaceConfig
	candidateMetadataSupplier func() map[string]model.ServerMetadata
	candidatesSupplier        func() *linkedhashset.Set

	selector           selectors.Selector[*single.Context, string]
	loadRatioAlgorithm selectors.LoadRatioAlgorithm

	quarantineNode *linkedhashset.Set
	triggerCh      chan struct{}
}

func (r *nodeBasedBalancer) Action() <-chan Action {
	return r.actionCh
}

func (r *nodeBasedBalancer) Close() error {
	close(r.triggerCh)
	r.cancel()
	r.Wait()
	return nil
}

func (r *nodeBasedBalancer) rebalanceEnsemble() {
	var err error
	swapGroup := &sync.WaitGroup{}
	defer swapGroup.Wait()

	currentStatus := r.statusSupplier()
	candidates := r.candidatesSupplier()
	groupedStatus := utils.GroupingShardsNodeByStatus(candidates, currentStatus)
	loadRatios := r.loadRatioAlgorithm(&model.RatioParams{NodeShardsInfos: groupedStatus})
	metadata := r.candidateMetadataSupplier()

	r.Info("start rebalance",
		slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
		slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
		slog.Float64("avg-shard-load-ratio", loadRatios.AvgShardLoadRatio()),
	)
	defer func() {
		r.Info("end rebalance",
			slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
			slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
			slog.Float64("avg-shard-load-ratio", loadRatios.AvgShardLoadRatio()),
		)
	}()

	// (1) deleted node
	for nodeIter := loadRatios.NodeLoadRatios.Iterator(); nodeIter.Next(); {
		var nodeLoadRatio *model.NodeLoadRatio
		var ok bool
		if nodeLoadRatio, ok = nodeIter.Value().(*model.NodeLoadRatio); !ok {
			panic("unexpected type cast")
		}
		if candidates.Contains(nodeLoadRatio.NodeID) {
			continue
		}
		deletedNodeID := nodeLoadRatio.NodeID
		for shardIter := nodeLoadRatio.ShardRatios.Iterator(); shardIter.Next(); {
			var shardRatio *model.ShardLoadRatio
			if shardRatio, ok = shardIter.Value().(*model.ShardLoadRatio); !ok {
				panic("unexpected type cast")
			}

			if err = r.swapShard(shardRatio, deletedNodeID, swapGroup, loadRatios, candidates, metadata, currentStatus); err != nil {
				r.Error("failed to select server when move ensemble out of deleted node",
					slog.String("namespace", shardRatio.Namespace),
					slog.Int64("shard", shardRatio.ShardID),
					slog.String("from-node", deletedNodeID),
					slog.Any("error", err),
				)
				continue
			}
		}
	}

	// (2) rebalance by load ratio
	if loadRatios.RatioGap() < loadRatios.AvgShardLoadRatio() {
		return
	}
	iter := loadRatios.NodeLoadRatios.Iterator()
	if !iter.Last() {
		return
	}
	var highestLoadRatioNode *model.NodeLoadRatio
	for {
		if highestLoadRatioNode = iter.Value().(*model.NodeLoadRatio); highestLoadRatioNode == nil {
			return // unexpected
		}
		if !r.IsNodeQuarantined(highestLoadRatioNode) {
			break
		}
		if iter.Prev() {
			continue
		}
		break
	}
	iter = highestLoadRatioNode.ShardRatios.Iterator()
	if !iter.Last() {
		return
	}
	for highestLoadRatioNode.Ratio-loadRatios.MinNodeLoadRatio() > loadRatios.AvgShardLoadRatio() {
		var highestLoadRatioShard *model.ShardLoadRatio
		if highestLoadRatioShard = iter.Value().(*model.ShardLoadRatio); highestLoadRatioShard == nil {
			break
		}
		if err = r.swapShard(highestLoadRatioShard, highestLoadRatioNode.NodeID, swapGroup, loadRatios, candidates, metadata, currentStatus); err != nil {
			r.Info("has no other choose to move the current shard ensemble to other node.",
				slog.String("namespace", highestLoadRatioShard.Namespace),
				slog.Int64("shard", highestLoadRatioShard.ShardID),
				slog.String("highest-load-node", highestLoadRatioNode.NodeID),
				slog.Any("error", err),
			)
			continue
		}
		if !iter.Prev() {
			break
		}
	}
	if highestLoadRatioNode.Ratio-loadRatios.MinNodeLoadRatio() > loadRatios.AvgShardLoadRatio() {
		r.quarantineNode.Add(highestLoadRatioNode.NodeID)
		r.Info("can't rebalance the current node, quarantine it.",
			slog.Float64("highest-load-node-ratio", highestLoadRatioNode.Ratio),
			slog.String("highest-load-node", highestLoadRatioNode.NodeID))
	}
}

func (r *nodeBasedBalancer) swapShard(
	candidateShard *model.ShardLoadRatio,
	fromNodeID string,
	swapGroup *sync.WaitGroup,
	loadRatios *model.RatioSnapshot,
	candidates *linkedhashset.Set,
	metadata map[string]model.ServerMetadata,
	currentStatus *model.ClusterStatus) error {
	nsc := r.namespaceConfigSupplier(candidateShard.Namespace)
	policies := nsc.Policies
	sContext := &single.Context{
		Candidates:         candidates,
		CandidatesMetadata: metadata,
		Policies:           policies,
		Status:             currentStatus,
		LoadRatioSupplier:  func() *model.RatioSnapshot { return loadRatios },
	}
	sContext.SetSelected(utils.FilterEnsemble(candidateShard.Ensemble, fromNodeID))
	var targetNodeId string
	var err error
	if targetNodeId, err = r.selector.Select(sContext); err != nil {
		return err
	}
	swapGroup.Add(1)
	r.actionCh <- &SwapNodeAction{
		Shard:  candidateShard.ShardID,
		From:   fromNodeID,
		To:     targetNodeId,
		waiter: swapGroup,
	}
	r.Info("propose to swap the shard", slog.Int64("shard", candidateShard.ShardID), slog.String("from", fromNodeID), slog.String("to", targetNodeId))
	loadRatios.MoveShardToNode(candidateShard, fromNodeID, targetNodeId)
	loadRatios.ReCalculateRatios()
	return nil
}

func (r *nodeBasedBalancer) IsNodeQuarantined(highestLoadRatioNode *model.NodeLoadRatio) bool {
	if r.quarantineNode.Contains(highestLoadRatioNode.NodeID) {
		// todo: improve it by high performance data structure
		needRefresh := false
		newHashSet := linkedhashset.New()
		// cleanup expired quarantine node
		iterator := r.quarantineNode.Iterator()
		for count := 0; iterator.Next(); count++ {
			nodeWithTs := iterator.Value().(*entities.TWithTimestamp[string]) //nolint:revive
			if time.Since(nodeWithTs.Timestamp) >= quarantineTime {
				continue
			}
			if count == 0 {
				break
			}
			needRefresh = true
			newHashSet.Add(nodeWithTs)
		}
		if !needRefresh {
			return true
		}
		r.quarantineNode = newHashSet
		if r.quarantineNode.Contains(highestLoadRatioNode.NodeID) {
			return true
		}
	}
	return false
}

func (r *nodeBasedBalancer) IsBalanced() bool {
	currentStatus := r.statusSupplier()
	candidates := r.candidatesSupplier()
	groupedStatus := utils.GroupingShardsNodeByStatus(candidates, currentStatus)
	loadRatios := r.loadRatioAlgorithm(&model.RatioParams{NodeShardsInfos: groupedStatus})
	return loadRatios.IsBalanced()
}

func (r *nodeBasedBalancer) Trigger() {
	r.Info("manually trigger balance")
	channel.PushNoBlock(r.triggerCh, triggerEvent)
}

func (r *nodeBasedBalancer) startBackgroundScheduler() {
	r.Add(1)
	go process.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-scheduler",
	}, func() {
		timer := time.NewTicker(loadBalancerScheduleInterval)
		defer timer.Stop()
		defer r.Done()
		for {
			select {
			case _, more := <-timer.C:
				if !more {
					return
				}
				channel.PushNoBlock(r.triggerCh, triggerEvent)
			case <-r.ctx.Done():
				return
			}
		}
	})
}

func (r *nodeBasedBalancer) startBackgroundNotifier() {
	r.Add(1)
	go process.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-notifier",
	}, func() {
		defer r.Done()
		for {
			select {
			case _, more := <-r.triggerCh:
				if !more {
					return
				}
				r.rebalanceEnsemble()
			case <-r.ctx.Done():
				return
			}
		}
	})
}

func NewLoadBalancer(options Options) LoadBalancer {
	ctx, cancelFunc := context.WithCancel(options.Context)
	// todo: add parameters
	logger := slog.With()
	nb := &nodeBasedBalancer{
		WaitGroup: &sync.WaitGroup{},
		Logger:    logger,

		ctx:                       ctx,
		cancel:                    cancelFunc,
		actionCh:                  make(chan Action, 1000),
		candidatesSupplier:        options.CandidatesSupplier,
		candidateMetadataSupplier: options.CandidateMetadataSupplier,
		namespaceConfigSupplier:   options.NamespaceConfigSupplier,
		statusSupplier:            options.StatusSupplier,
		selector:                  single.NewSelector(),
		loadRatioAlgorithm:        single.DefaultShardsRank,
		quarantineNode:            linkedhashset.New(),
		triggerCh:                 make(chan struct{}, 1),
	}
	nb.startBackgroundScheduler()
	nb.startBackgroundNotifier()
	return nb
}
