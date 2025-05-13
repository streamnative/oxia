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

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/channel"
	"github.com/streamnative/oxia/common/entities"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/selectors"
	"github.com/streamnative/oxia/coordinator/selectors/single"
	"github.com/streamnative/oxia/coordinator/utils"
)

var _ LoadBalancer = &nodeBasedBalancer{}

type nodeBasedBalancer struct {
	ctx    context.Context
	cancel context.CancelFunc
	latch  *sync.WaitGroup
	log    *slog.Logger

	actionCh chan Action

	clusterStatusSupplier    func() *model.ClusterStatus
	namespaceConfigSupplier  func(namespace string) *model.NamespaceConfig
	metadataSupplier         func() map[string]model.ServerMetadata
	clusterServerIDsSupplier func() *linkedhashset.Set

	selector           selectors.Selector[*single.Context, *string]
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
	r.latch.Wait()
	return nil
}

func (r *nodeBasedBalancer) rebalanceEnsemble(currentStatus *model.ClusterStatus, shardsGroupingByNode map[string][]model.ShardInfo) {
	var err error
	swapGroup := &sync.WaitGroup{}
	serverIDs := r.clusterServerIDsSupplier()
	metadata := r.metadataSupplier()
	loadRatios := r.loadRatioAlgorithm(&model.RatioParams{NodeShardsInfos: shardsGroupingByNode})

	// (1) deleted node
	for nodeIter := loadRatios.NodeLoadRatios().Iterator(); nodeIter.Next(); {
		var nodeLoadRatio *model.NodeLoadRatio
		var ok bool
		if nodeLoadRatio, ok = nodeIter.Value().(*model.NodeLoadRatio); !ok {
			panic("unexpected type cast")
		}
		if serverIDs.Contains(nodeLoadRatio.NodeID) {
			continue
		}
		deletedNodeID := nodeLoadRatio.NodeID
		for shardIter := nodeLoadRatio.ShardRatios.Iterator(); shardIter.Next(); {
			var shardRatio *model.ShardLoadRatio
			if shardRatio, ok = shardIter.Value().(*model.ShardLoadRatio); !ok {
				panic("unexpected type cast")
			}

			if err = r.swapShard(shardRatio, deletedNodeID, swapGroup, loadRatios, serverIDs, metadata, currentStatus); err != nil {
				r.log.Error("failed to select server when move ensemble out of deleted node",
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
	if loadRatios.RatioGap() < loadGapRatio {
		return
	}
	var highestLoadRatioNode *model.NodeLoadRatio
	for {
		if highestLoadRatioNode = loadRatios.DequeueHighestNode(); highestLoadRatioNode == nil {
			return // unexpected
		}
		if r.IsNodeQuarantined(highestLoadRatioNode) {
			continue
		}
		break
	}
	for loadRatios.RatioGap() >= loadGapRatio {
		var highestLoadRatioShard *model.ShardLoadRatio
		if highestLoadRatioShard = highestLoadRatioNode.DequeueHighestShard(); highestLoadRatioShard == nil {
			return
		}
		if err = r.swapShard(highestLoadRatioShard, highestLoadRatioNode.NodeID, swapGroup, loadRatios, serverIDs, metadata, currentStatus); err != nil {
			r.log.Info("has no other choose to move the current shard ensemble to other node.",
				slog.String("namespace", highestLoadRatioShard.Namespace),
				slog.Int64("shard", highestLoadRatioShard.ShardID),
				slog.String("highest-load-node", highestLoadRatioNode.NodeID),
				slog.Any("error", err),
			)
			continue
		}
	}
	if loadRatios.RatioGap() >= loadGapRatio {
		r.quarantineNode.Add(highestLoadRatioNode.NodeID)
		r.log.Info("can't rebalance the current node, quarantine it.",
			slog.Float64("highest-load-node-ratio", highestLoadRatioNode.Ratio),
			slog.String("highest-load-node", highestLoadRatioNode.NodeID))
	}
	swapGroup.Wait()
}

func (r *nodeBasedBalancer) swapShard(
	candidateShard *model.ShardLoadRatio,
	fromNodeID string,
	swapGroup *sync.WaitGroup,
	loadRatios *model.Ratio,
	serverIDs *linkedhashset.Set,
	metadata map[string]model.ServerMetadata,
	currentStatus *model.ClusterStatus) error {
	nsc := r.namespaceConfigSupplier(candidateShard.Namespace)
	policies := nsc.Policies
	sContext := &single.Context{
		Candidates:         serverIDs,
		CandidatesMetadata: metadata,
		Policies:           policies,
		Status:             currentStatus,
		LoadRatioSupplier:  func() *model.Ratio { return loadRatios },
	}
	sContext.SetSelected(utils.FilterEnsemble(candidateShard.Ensemble, fromNodeID))
	var targetNodeId *string
	var err error
	if targetNodeId, err = r.selector.Select(sContext); err != nil {
		return err
	}
	swapGroup.Add(1)
	tnID := *targetNodeId
	r.actionCh <- &SwapNodeAction{
		Shard:  candidateShard.ShardID,
		From:   fromNodeID,
		To:     tnID,
		waiter: swapGroup,
	}
	loadRatios.MoveShardToNode(candidateShard, tnID)
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

func (r *nodeBasedBalancer) startBackgroundScheduler() {
	r.latch.Add(1)
	go common.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-scheduler",
	}, func() {
		for {
			timer := time.NewTimer(loadBalancerScheduleInterval)
			defer timer.Stop()
			defer r.latch.Done()
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
	r.latch.Add(1)
	go common.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-notifier",
	}, func() {
		for {
			defer r.latch.Done()
			select {
			case _, more := <-r.triggerCh:
				if !more {
					return
				}
				currentClusterStatus := r.clusterStatusSupplier()
				groupedStatus := utils.GroupingShardsNodeByStatus(r.clusterStatusSupplier())
				r.rebalanceEnsemble(currentClusterStatus, groupedStatus)
			case <-r.ctx.Done():
				return
			}
		}
	})
}

func NewLoadBalancer(options Options) LoadBalancer {
	ctx, cancelFunc := context.WithCancel(options.Context)
	logger := slog.With()
	nb := &nodeBasedBalancer{
		ctx:                      ctx,
		cancel:                   cancelFunc,
		latch:                    &sync.WaitGroup{},
		log:                      logger,
		actionCh:                 make(chan Action, 1000),
		clusterStatusSupplier:    options.ClusterStatusSupplier,
		namespaceConfigSupplier:  options.NamespaceConfigSupplier,
		metadataSupplier:         options.MetadataSupplier,
		clusterServerIDsSupplier: options.ClusterServerIDsSupplier,
		selector:                 single.NewSelector(),
		loadRatioAlgorithm:       single.DefaultShardsRank,
		quarantineNode:           linkedhashset.New(),
		triggerCh:                make(chan struct{}, 1),
	}
	nb.startBackgroundScheduler()
	nb.startBackgroundNotifier()
	return nb
}
