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
	clusterServerIdsSupplier func() *linkedhashset.Set

	selector           selectors.Selector[*single.Context, *string]
	loadRatioAlgorithm LoadRatioAlgorithm

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

func (r *nodeBasedBalancer) rebalanceEnsemble(currentStatus *model.ClusterStatus, shardsGroupingByNode map[string][]shardInfo) {
	var err error
	var tmpI interface{}
	var ok bool
	swapGroup := &sync.WaitGroup{}

	serverIds := r.clusterServerIdsSupplier()
	metadata := r.metadataSupplier()
	for maybeDeletedNodeId, shardInfos := range shardsGroupingByNode {
		if !serverIds.Contains(maybeDeletedNodeId) {
			continue
		}
		for _, si := range shardInfos {
			nsc := r.namespaceConfigSupplier(si.namespace)
			policies := nsc.Policies
			sContext := &single.Context{
				Candidates:         serverIds,
				CandidatesMetadata: metadata,
				Policies:           policies,
				Status:             currentStatus,
			}
			sContext.SetSelected(filterEnsemble(si.ensemble, maybeDeletedNodeId))
			var targetNodeId *string
			if targetNodeId, err = r.selector.Select(sContext); err != nil {
				r.log.Error("failed to select server when move ensemble out of deleted node",
					slog.String("namespace", si.namespace),
					slog.Int64("shard", si.shardID),
					slog.String("deleted-node", maybeDeletedNodeId),
					slog.Any("error", err),
				)
				continue
			}
			swapGroup.Add(1)
			r.actionCh <- &SwapNodeAction{
				Shard:  si.shardID,
				From:   maybeDeletedNodeId,
				To:     *targetNodeId,
				waiter: swapGroup,
			}
		}
	}
	loadRatios := r.loadRatioAlgorithm(&LoadRatioParams{shardsGroupingByNode})
	if (loadRatios.maxNodeLoadRatio - loadRatios.minNodeLoadRatio) < loadGapRatio {
		// todo: improve here
		r.log.Info("load gap is less than threshold, no need to rebalance")
		return
	}
	var highestLoadRatioNode *NodeLoadRatio
	for {
		if tmpI, ok = loadRatios.nodeLoadRatios.Dequeue(); !ok {
			return
		}
		highestLoadRatioNode = tmpI.(*NodeLoadRatio)
		if r.IsNodeQuarantined(highestLoadRatioNode) {
			continue
		}
		break
	}

	var currentRatio = loadRatios.maxNodeLoadRatio
	for (currentRatio - loadRatios.minNodeLoadRatio) >= loadGapRatio {
		if tmpI, ok = highestLoadRatioNode.shardRatios.Dequeue(); !ok {
			return
		}
		highestLoadRatioShard := tmpI.(*ShardLoadRatio)
		ns := highestLoadRatioShard.namespace
		fromNodeID := highestLoadRatioNode.nodeID
		sContext := &single.Context{
			Candidates:         serverIds,
			CandidatesMetadata: metadata,
			Policies:           r.namespaceConfigSupplier(ns).Policies,
			Status:             currentStatus,
		}
		sContext.SetSelected(filterEnsemble(highestLoadRatioShard.ensemble, fromNodeID))
		var targetNodeId *string
		if targetNodeId, err = r.selector.Select(sContext); err != nil {
			r.log.Error("failed to select server when do rebalance",
				slog.String("namespace", highestLoadRatioShard.namespace),
				slog.Int64("shard", highestLoadRatioShard.shardID),
				slog.String("highest-load-node", highestLoadRatioNode.nodeID),
				slog.Any("error", err),
			)
			continue
		}
		if *targetNodeId == fromNodeID {
			r.log.Info("has no other choose to move the current shard ensemble to other node.",
				slog.String("namespace", highestLoadRatioShard.namespace),
				slog.Int64("shard", highestLoadRatioShard.shardID),
				slog.String("highest-load-node", highestLoadRatioNode.nodeID),
				slog.Any("error", err),
			)
			continue
		}
		swapGroup.Add(1)
		r.actionCh <- &SwapNodeAction{
			Shard:  highestLoadRatioShard.shardID,
			From:   fromNodeID,
			To:     *targetNodeId,
			waiter: swapGroup,
		}
	}
	if (currentRatio - loadRatios.minNodeLoadRatio) >= loadGapRatio {
		r.quarantineNode.Add(highestLoadRatioNode.nodeID)
		r.log.Info("can't rebalance the current node, quarantine it.",
			slog.Float64("highest-load-node-ratio", highestLoadRatioNode.ratio),
			slog.String("highest-load-node", highestLoadRatioNode.nodeID))
	}
	swapGroup.Wait()
}

func (r *nodeBasedBalancer) IsNodeQuarantined(highestLoadRatioNode *NodeLoadRatio) bool {
	if r.quarantineNode.Contains(highestLoadRatioNode.nodeID) {
		// todo: improve it by high performance data structure
		needRefresh := false
		newHashSet := linkedhashset.New()
		// cleanup expired quarantine node
		iterator := r.quarantineNode.Iterator()
		for count := 0; iterator.Next(); count++ {
			nodeWithTs := iterator.Value().(*entities.TWithTimestamp[string])
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
		if r.quarantineNode.Contains(highestLoadRatioNode.nodeID) {
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

func (r *nodeBasedBalancer) startBackgroundWorker() {
	r.latch.Add(1)
	go common.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-worker",
	}, func() {
		for {
			defer r.latch.Done()
			select {
			case _, more := <-r.triggerCh:
				if !more {
					return
				}
				currentClusterStatus := r.clusterStatusSupplier()
				groupedStatus := groupingShardsNodeByStatus(r.clusterStatusSupplier())
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
		clusterServerIdsSupplier: options.ClusterServerIdsSupplier,
		selector:                 single.NewSelector(),
		loadRatioAlgorithm:       DefaultShardsRank,
		quarantineNode:           linkedhashset.New(),
		triggerCh:                make(chan struct{}),
	}
	nb.startBackgroundScheduler()
	nb.startBackgroundWorker()
	return nb
}
