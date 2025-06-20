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

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/coordinator/resources"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/selectors"
	"github.com/oxia-db/oxia/coordinator/selectors/single"
	"github.com/oxia-db/oxia/coordinator/utils"
)

var _ LoadBalancer = &nodeBasedBalancer{}

type nodeBasedBalancer struct {
	*slog.Logger
	*sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	scheduleInterval time.Duration
	quarantineTime   time.Duration

	statusResource resources.StatusResource
	configResource resources.ClusterConfigResource

	selector           selectors.Selector[*single.Context, string]
	loadRatioAlgorithm selectors.LoadRatioAlgorithm
	quarantineNodeMap  sync.Map

	actionCh  chan Action
	triggerCh chan struct{}
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

func (r *nodeBasedBalancer) quarantineNodes() *linkedhashset.Set[string] {
	nodes := linkedhashset.New[string]()
	r.quarantineNodeMap.Range(func(nodeID, _ any) bool {
		nodes.Add(nodeID.(string))
		return true
	})
	return nodes
}

func (r *nodeBasedBalancer) rebalanceEnsemble() {
	r.checkQuarantineNodes()

	swapGroup := &sync.WaitGroup{}
	currentStatus := r.statusResource.Load()
	candidates, metadata := r.configResource.NodesWithMetadata()
	groupedStatus, historyNodes := utils.GroupingShardsNodeByStatus(candidates, currentStatus)
	loadRatios := r.loadRatioAlgorithm(&model.RatioParams{NodeShardsInfos: groupedStatus, HistoryNodes: historyNodes})

	r.Info("start rebalance",
		slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
		slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
		slog.Any("quarantine-nodes", r.quarantineNodes()),
		slog.Float64("avg-shard-ratio", loadRatios.AvgShardLoadRatio()),
	)

	defer func() {
		swapGroup.Wait()
		r.Info("end rebalance",
			slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
			slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
			slog.Float64("avg-shard-ratio", loadRatios.AvgShardLoadRatio()),
		)
	}()

	r.cleanDeletedNode(loadRatios, candidates, metadata, currentStatus, swapGroup)

	r.balanceHighestNode(loadRatios, candidates, metadata, currentStatus, swapGroup)
}

func (r *nodeBasedBalancer) balanceHighestNode(loadRatios *model.Ratio, candidates *linkedhashset.Set[string], metadata map[string]model.ServerMetadata, currentStatus *model.ClusterStatus, swapGroup *sync.WaitGroup) {
	if loadRatios.RatioGap() <= loadRatios.AvgShardLoadRatio() {
		return
	}
	nodeIter := loadRatios.NodeIterator()
	if !nodeIter.Last() {
		return
	}
	var highestLoadRatioNode *model.NodeLoadRatio
	for {
		if highestLoadRatioNode = nodeIter.Value(); highestLoadRatioNode == nil {
			return // unexpected
		}
		if !r.IsNodeQuarantined(highestLoadRatioNode) {
			break
		}
		if nodeIter.Prev() {
			continue
		}
		break
	}
	shardIter := highestLoadRatioNode.ShardIterator()
	if !shardIter.Last() {
		return
	}
	for highestLoadRatioNode.Ratio-loadRatios.MinNodeLoadRatio() > loadRatios.AvgShardLoadRatio() {
		var highestLoadRatioShard *model.ShardLoadRatio
		if highestLoadRatioShard = shardIter.Value(); highestLoadRatioShard == nil {
			break
		}
		fromNodeID := highestLoadRatioNode.NodeID
		fromNode := highestLoadRatioNode.Node
		if _, err := r.swapShard(highestLoadRatioShard, fromNode, swapGroup, loadRatios, candidates, metadata, currentStatus); err != nil {
			r.Error("failed to select server when swap the node",
				slog.String("namespace", highestLoadRatioShard.Namespace),
				slog.Int64("shard", highestLoadRatioShard.ShardID),
				slog.String("from-node", fromNodeID),
				slog.Any("error", err),
			)
			continue
		}
		if !shardIter.Prev() {
			break
		}
	}
	if highestLoadRatioNode.Ratio-loadRatios.MinNodeLoadRatio() > loadRatios.AvgShardLoadRatio() {
		r.quarantineNodeMap.Store(highestLoadRatioNode.NodeID, time.Now())
		r.Info("can't rebalance the current node, quarantine it.",
			slog.Float64("highest-load-node-ratio", highestLoadRatioNode.Ratio),
			slog.String("highest-load-node", highestLoadRatioNode.NodeID))
	}
}

func (r *nodeBasedBalancer) cleanDeletedNode(loadRatios *model.Ratio,
	candidates *linkedhashset.Set[string],
	metadata map[string]model.ServerMetadata,
	currentStatus *model.ClusterStatus,
	swapGroup *sync.WaitGroup) {
	for nodeIter := loadRatios.NodeIterator(); nodeIter.Next(); {
		nodeLoadRatio := nodeIter.Value()
		if candidates.Contains(nodeLoadRatio.NodeID) {
			continue
		}
		deletedNodeID := nodeLoadRatio.NodeID
		deletedNode := nodeLoadRatio.Node
		for shardIter := nodeLoadRatio.ShardIterator(); shardIter.Next(); {
			var shardRatio = shardIter.Value()
			if swapped, err := r.swapShard(shardRatio, deletedNode, swapGroup, loadRatios, candidates, metadata, currentStatus); err != nil || !swapped {
				r.Error("failed to select server when move ensemble out of deleted node",
					slog.String("namespace", shardRatio.Namespace),
					slog.Int64("shard", shardRatio.ShardID),
					slog.String("from-node", deletedNodeID),
					slog.Any("error", err),
				)
				continue
			}
		}
		if err := loadRatios.RemoveDeletedNode(nodeLoadRatio.NodeID); err != nil {
			r.Error("failed to remove deleted node from ratio snapshot", slog.Any("error", err))
		}
	}
}

func (r *nodeBasedBalancer) swapShard(
	candidateShard *model.ShardLoadRatio,
	fromNode model.Server,
	swapGroup *sync.WaitGroup,
	loadRatios *model.Ratio,
	candidates *linkedhashset.Set[string],
	metadata map[string]model.ServerMetadata,
	currentStatus *model.ClusterStatus) (bool, error) {
	var nsc *model.NamespaceConfig
	var exist bool
	var err error

	if nsc, exist = r.configResource.NamespaceConfig(candidateShard.Namespace); !exist {
		return false, nil
	}
	policies := nsc.Policies
	sContext := &single.Context{
		Candidates:         candidates,
		CandidatesMetadata: metadata,
		Policies:           policies,
		Status:             currentStatus,
		LoadRatioSupplier:  func() *model.Ratio { return loadRatios },
	}
	fromNodeID := fromNode.GetIdentifier()

	// filter selected
	selected := linkedhashset.New[string]()
	for _, candidate := range candidateShard.Ensemble {
		candidateID := candidate.GetIdentifier()
		if candidateID == fromNodeID {
			continue
		}
		selected.Add(candidateID)
	}
	sContext.SetSelected(selected)

	var targetNodeID string
	if targetNodeID, err = r.selector.Select(sContext); err != nil {
		return false, err
	}
	if targetNodeID == fromNodeID {
		return false, nil
	}
	var targetNode *model.Server
	if targetNode, exist = r.configResource.Node(targetNodeID); !exist {
		return false, errors.New("target node does not exist")
	}

	swapGroup.Add(1)
	r.actionCh <- &SwapNodeAction{
		Shard:  candidateShard.ShardID,
		From:   fromNode,
		To:     *targetNode,
		waiter: swapGroup,
	}
	r.Info("propose to swap the shard", slog.Int64("shard", candidateShard.ShardID), slog.Any("from", fromNode), slog.Any("to", targetNodeID))
	loadRatios.MoveShardToNode(candidateShard, fromNodeID, targetNodeID)
	loadRatios.ReCalculateRatios()
	return true, nil
}

func (r *nodeBasedBalancer) checkQuarantineNodes() {
	deletedKeys := make([]string, 0)
	r.quarantineNodeMap.Range(func(key, value any) bool {
		timestamp := value.(time.Time) //nolint: revive
		if time.Since(timestamp) >= r.quarantineTime {
			deletedKeys = append(deletedKeys, key.(string))
		}
		return true
	})
	for _, key := range deletedKeys {
		r.quarantineNodeMap.Delete(key)
	}
}

func (r *nodeBasedBalancer) IsNodeQuarantined(highestLoadRatioNode *model.NodeLoadRatio) bool {
	_, found := r.quarantineNodeMap.Load(highestLoadRatioNode.NodeID)
	return found
}

func (r *nodeBasedBalancer) IsBalanced() bool {
	status := r.statusResource.Load()
	candidates := r.configResource.Nodes()
	groupedStatus, historyNodes := utils.GroupingShardsNodeByStatus(candidates, status)
	return r.loadRatioAlgorithm(
		&model.RatioParams{
			NodeShardsInfos: groupedStatus,
			HistoryNodes:    historyNodes,
			QuarantineNodes: r.quarantineNodes(),
		},
	).IsBalanced()
}

func (r *nodeBasedBalancer) Trigger() {
	r.Info("manually trigger balance")
	channel.PushNoBlock(r.triggerCh, triggerEvent)
}

func (r *nodeBasedBalancer) LoadRatioAlgorithm() selectors.LoadRatioAlgorithm {
	return r.loadRatioAlgorithm
}

func (r *nodeBasedBalancer) startBackgroundScheduler() {
	r.Add(1)
	go process.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-scheduler",
	}, func() {
		timer := time.NewTicker(r.scheduleInterval)
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
	logger := slog.With(
		slog.String("component", "load-balancer"))

	if options.ScheduleInterval == 0 {
		options.ScheduleInterval = defaultLoadBalancerScheduleInterval
	}
	if options.QuarantineTime == 0 {
		options.QuarantineTime = defaultQuarantineTime
	}

	nb := &nodeBasedBalancer{
		WaitGroup:        &sync.WaitGroup{},
		Logger:           logger,
		scheduleInterval: options.ScheduleInterval,
		quarantineTime:   options.QuarantineTime,
		ctx:              ctx,
		cancel:           cancelFunc,
		actionCh:         make(chan Action, 1000),

		statusResource:     options.StatusResource,
		configResource:     options.ClusterConfigResource,
		selector:           single.NewSelector(),
		loadRatioAlgorithm: single.DefaultShardsRank,
		quarantineNodeMap:  sync.Map{},
		triggerCh:          make(chan struct{}, 1),
	}
	nb.startBackgroundScheduler()
	nb.startBackgroundNotifier()
	return nb
}
