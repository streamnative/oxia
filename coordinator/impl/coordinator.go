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
	"context"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"

	"github.com/streamnative/oxia/coordinator/utils"

	"github.com/streamnative/oxia/common/process"

	"github.com/streamnative/oxia/coordinator/balancer"

	"github.com/streamnative/oxia/common/concurrent"

	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/coordinator/selectors/ensemble"

	"github.com/streamnative/oxia/coordinator/selectors"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/proto"
)

var (
	ErrNamespaceNotFound = errors.New("namespace not found")
)

type ShardAssignmentsProvider interface {
	WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error)
}

type NodeAvailabilityListener interface {
	NodeBecameUnavailable(node model.Server)
}

type Coordinator interface {
	io.Closer

	ShardAssignmentsProvider

	InitiateLeaderElection(namespace string, shard int64, metadata model.ShardMetadata) error
	ElectedLeader(namespace string, shard int64, metadata model.ShardMetadata) error
	ShardDeleted(namespace string, shard int64) error

	NodeAvailabilityListener
	ClusterStatus() model.ClusterStatus

	// FindServerByIdentifier searches for a server in the cluster by its identifier and returns it if found.
	FindServerByIdentifier(identifier string) (*model.Server, bool)

	TriggerBalance()

	IsBalanced() bool
}

type coordinator struct {
	sync.RWMutex
	assignmentsChanged concurrent.ConditionContext

	loadBalancer     balancer.LoadBalancer
	ensembleSelector selectors.Selector[*ensemble.Context, []string]

	MetadataProvider
	clusterConfigProvider func() (model.ClusterConfig, error)
	model.ClusterConfig

	serverIndexesOnce     sync.Once
	serverIDs             *linkedhashset.Set[string]
	serverIDIndex         map[string]*model.Server
	drainingServerIDIndex map[string]*model.Server

	clusterConfigChangeCh chan any

	shardControllers map[int64]ShardController
	nodeControllers  map[string]NodeController

	// Draining nodes are nodes that were removed from the
	// nodes list. We keep sending them assignments updates
	// because they might be still reachable to clients.
	drainingNodes   map[string]NodeController
	clusterStatus   *model.ClusterStatus
	assignments     *proto.ShardAssignments
	metadataVersion Version
	rpc             RpcProvider
	log             *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

func NewCoordinator(metadataProvider MetadataProvider,
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigNotificationsCh chan any,
	rpc RpcProvider) (Coordinator, error) {
	initialClusterConf, err := clusterConfigProvider()
	if err != nil {
		return nil, err
	}

	c := &coordinator{
		MetadataProvider:      metadataProvider,
		clusterConfigProvider: clusterConfigProvider,
		clusterConfigChangeCh: clusterConfigNotificationsCh,
		ClusterConfig:         initialClusterConf,
		ensembleSelector:      ensemble.NewSelector(),
		shardControllers:      make(map[int64]ShardController),
		nodeControllers:       make(map[string]NodeController),
		drainingNodes:         make(map[string]NodeController),
		rpc:                   rpc,
		log: slog.With(
			slog.String("component", "coordinator"),
		),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.assignmentsChanged = concurrent.NewConditionContext(c)

	c.clusterStatus, c.metadataVersion, err = metadataProvider.Get()
	if err != nil && !errors.Is(err, ErrMetadataNotInitialized) {
		return nil, err
	}

	for _, sa := range c.Servers {
		c.nodeControllers[sa.GetIdentifier()] = NewNodeController(sa, c, c, c.rpc)
	}

	c.loadBalancer = balancer.NewLoadBalancer(balancer.Options{
		Context: c.ctx,
		StatusSupplier: func() *model.ClusterStatus {
			c.RLock()
			defer c.RUnlock()
			return c.clusterStatus
		},
		CandidateMetadataSupplier: func() map[string]model.ServerMetadata {
			c.RLock()
			defer c.RUnlock()
			return c.ServerMetadata
		},
		CandidatesSupplier: func() *linkedhashset.Set[string] {
			c.RLock()
			defer c.RUnlock()
			return c.ServerIDs()
		},
		NamespaceConfigSupplier: func(namespace string) *model.NamespaceConfig {
			c.RLock()
			defer c.RUnlock()
			return GetNamespaceConfig(c.Namespaces, namespace)
		},
	})

	if c.clusterStatus == nil {
		// Before initializing the cluster, it's better to make sure we
		// have all the nodes available, otherwise the coordinator might be
		// the first component in getting started and will print out a lot
		// of error logs regarding failed leader elections
		c.waitForAllNodesToBeAvailable()

		if err = c.initialAssignment(); err != nil {
			return nil, err
		}
	} else {
		if err = c.applyNewClusterConfig(); err != nil {
			return nil, err
		}
	}

	c.initialShardController(&initialClusterConf)

	go process.DoWithLabels(c.ctx, map[string]string{
		"component": "coordinator-action-worker",
	}, c.startBackgroundActionWorker)

	go process.DoWithLabels(c.ctx, map[string]string{
		"oxia": "coordinator-wait-for-events",
	}, c.waitForExternalEvents)

	return c, nil
}

func (c *coordinator) IsBalanced() bool {
	return c.loadBalancer.IsBalanced()
}

func (c *coordinator) ServerIDs() *linkedhashset.Set[string] {
	c.maybeLoadServerIndex()
	return c.serverIDs
}

func (c *coordinator) DrainingServerIDIndex() map[string]*model.Server {
	c.maybeLoadServerIndex()
	return c.drainingServerIDIndex
}

func (c *coordinator) ServerIDIndex() map[string]*model.Server {
	c.maybeLoadServerIndex()
	return c.serverIDIndex
}

func (c *coordinator) TriggerBalance() {
	c.loadBalancer.Trigger()
}

//nolint:revive
func (c *coordinator) maybeLoadServerIndex() {
	c.serverIndexesOnce.Do(func() {
		ids := linkedhashset.New[string]()
		idIndex := make(map[string]*model.Server)
		drainingIndex := make(map[string]*model.Server)

		for idx := range c.Servers {
			server := &c.Servers[idx]
			identifier := server.GetIdentifier()
			ids.Add(identifier)
			idIndex[identifier] = server
		}

		if c.clusterStatus != nil {
			// todo: improve the index here
			for _, namespace := range c.clusterStatus.Namespaces {
				for _, shard := range namespace.Shards {
					for idx, sv := range shard.Ensemble {
						if _, exist := idIndex[sv.GetIdentifier()]; !exist {
							drainingIndex[sv.GetIdentifier()] = &shard.Ensemble[idx]
						}
					}
					for idx, sv := range shard.RemovedNodes {
						if _, exist := idIndex[sv.GetIdentifier()]; !exist {
							drainingIndex[sv.GetIdentifier()] = &shard.RemovedNodes[idx]
						}
					}
				}
			}
		}

		c.drainingServerIDIndex = drainingIndex
		c.serverIDIndex = idIndex
		c.serverIDs = ids
	})
}

func (c *coordinator) allUnavailableNodes() []string {
	nodes := []string{}
	for nodeName, nc := range c.nodeControllers {
		if nc.Status() != Running {
			nodes = append(nodes, nodeName)
		}
	}

	return nodes
}

func (c *coordinator) initialShardController(initialClusterConf *model.ClusterConfig) {
	for ns, shards := range c.clusterStatus.Namespaces {
		for shard, shardMetadata := range shards.Shards {
			namespaceConfig := GetNamespaceConfig(initialClusterConf.Namespaces, ns)
			c.shardControllers[shard] = NewShardController(ns, shard, namespaceConfig, shardMetadata, c.rpc, c)
		}
	}
}

func (c *coordinator) waitForAllNodesToBeAvailable() {
	c.log.Info("Waiting for all the nodes to be available")
	for {
		select {
		case <-time.After(1 * time.Second):
			c.log.Info("Start to check unavailable nodes")

			unavailableNodes := c.allUnavailableNodes()
			if len(unavailableNodes) == 0 {
				c.log.Info("All nodes are now available")
				return
			}

			c.log.Info(
				"A part of nodes is not available",
				slog.Any("UnavailableNodeNames", unavailableNodes),
			)
		case <-c.ctx.Done():
			// Give up if we're closing the coordinator
			return
		}
	}
}

// selectNewEnsemble select a new server ensemble based on namespace policies and current cluster status.
// It uses the ensemble selector to choose appropriate servers and returns the selected server metadata or an error.
func (c *coordinator) selectNewEnsemble(ns *model.NamespaceConfig, editingStatus *model.ClusterStatus) ([]model.Server, error) {
	ensembleContext := &ensemble.Context{
		Candidates:         c.ServerIDs(),
		CandidatesMetadata: c.ServerMetadata,
		Policies:           ns.Policies,
		Status:             editingStatus,
		Replicas:           int(ns.ReplicationFactor),
		LoadRatioSupplier: func() *model.Ratio {
			groupedStatus := utils.GroupingShardsNodeByStatus(c.ServerIDs(), editingStatus)
			return c.loadBalancer.LoadRatioAlgorithm()(&model.RatioParams{NodeShardsInfos: groupedStatus})
		},
	}
	var ensembleIDs []string
	var err error
	if ensembleIDs, err = c.ensembleSelector.Select(ensembleContext); err != nil {
		return nil, err
	}
	esm := make([]model.Server, 0)
	for _, id := range ensembleIDs {
		server := c.ServerIDIndex()[id]
		esm = append(esm, *server)
	}
	return esm, nil
}

// Assign the shards to the available servers.
func (c *coordinator) initialAssignment() error {
	c.log.Info(
		"Performing initial assignment",
		slog.Any("clusterConfig", c.ClusterConfig),
	)

	clusterStatus, _, _ := applyClusterChanges(&c.ClusterConfig, model.NewClusterStatus(), c.selectNewEnsemble)

	var err error
	if c.metadataVersion, err = c.Store(clusterStatus, MetadataNotExists); err != nil {
		return err
	}

	c.clusterStatus = clusterStatus
	return nil
}

func (c *coordinator) applyNewClusterConfig() error {
	c.log.Info(
		"Checking cluster config",
		slog.Any("clusterConfig", c.ClusterConfig),
		slog.Any("metadataVersion", c.metadataVersion),
	)

	clusterStatus, shardsToAdd, shardsToDelete := applyClusterChanges(&c.ClusterConfig, c.clusterStatus, c.selectNewEnsemble)

	if len(shardsToAdd) > 0 || len(shardsToDelete) > 0 {
		var err error

		if c.metadataVersion, err = c.Store(clusterStatus, c.metadataVersion); err != nil {
			return err
		}
	}

	c.clusterStatus = clusterStatus
	return nil
}

func (c *coordinator) Close() error {
	var err error

	for _, sc := range c.shardControllers {
		err = multierr.Append(err, sc.Close())
	}

	for _, nc := range c.nodeControllers {
		err = multierr.Append(err, nc.Close())
	}

	for _, nc := range c.drainingNodes {
		err = multierr.Append(err, nc.Close())
	}
	return err
}

func (c *coordinator) NodeBecameUnavailable(node model.Server) {
	c.Lock()

	if nc, ok := c.drainingNodes[node.GetIdentifier()]; ok {
		// The draining node became unavailable. Let's remove it
		delete(c.drainingNodes, node.GetIdentifier())
		_ = nc.Close()
	}

	ctrls := make(map[int64]ShardController)
	for k, sc := range c.shardControllers {
		ctrls[k] = sc
	}
	c.Unlock()

	for _, sc := range ctrls {
		sc.HandleNodeFailure(node)
	}
}

func (c *coordinator) WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error) {
	c.Lock()
	defer c.Unlock()

	for pb.Equal(currentValue, c.assignments) {
		// Wait on the condition until the assignments get changed
		if err := c.assignmentsChanged.Wait(ctx); err != nil {
			return nil, err
		}
	}

	return c.assignments, nil
}

func (c *coordinator) InitiateLeaderElection(namespace string, shard int64, metadata model.ShardMetadata) error {
	c.Lock()
	defer c.Unlock()

	cs := c.clusterStatus.Clone()
	ns, ok := cs.Namespaces[namespace]
	if !ok {
		return ErrNamespaceNotFound
	}

	ns.Shards[shard] = metadata
	newMetadataVersion, err := c.Store(cs, c.metadataVersion)
	if err != nil {
		return err
	}

	c.metadataVersion = newMetadataVersion
	c.clusterStatus = cs
	return nil
}

func (c *coordinator) ElectedLeader(namespace string, shard int64, metadata model.ShardMetadata) error {
	c.Lock()
	defer c.Unlock()

	cs := c.clusterStatus.Clone()
	ns, ok := cs.Namespaces[namespace]
	if !ok {
		return ErrNamespaceNotFound
	}

	ns.Shards[shard] = metadata
	newMetadataVersion, err := c.Store(cs, c.metadataVersion)
	if err != nil {
		return err
	}

	c.metadataVersion = newMetadataVersion
	c.clusterStatus = cs

	c.computeNewAssignments()
	return nil
}

func (c *coordinator) ShardDeleted(namespace string, shard int64) error {
	c.Lock()
	defer c.Unlock()

	cs := c.clusterStatus.Clone()
	ns, ok := cs.Namespaces[namespace]
	if !ok {
		return ErrNamespaceNotFound
	}

	delete(ns.Shards, shard)
	if len(ns.Shards) == 0 {
		delete(cs.Namespaces, namespace)
	}

	newMetadataVersion, err := c.Store(cs, c.metadataVersion)
	if err != nil {
		return err
	}

	c.metadataVersion = newMetadataVersion
	c.clusterStatus = cs

	c.computeNewAssignments()
	return nil
}

// This is called while already holding the lock on the coordinator.
func (c *coordinator) computeNewAssignments() {
	c.assignments = &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{},
	}

	// Update the leader for the shards on all the namespaces
	for name, ns := range c.clusterStatus.Namespaces {
		nsAssignments := &proto.NamespaceShardsAssignment{
			Assignments:    make([]*proto.ShardAssignment, 0),
			ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
		}

		for shard, a := range ns.Shards {
			var leader string
			if a.Leader != nil {
				leader = a.Leader.Public
			}
			if a.Status != model.ShardStatusDeleting {
				nsAssignments.Assignments = append(nsAssignments.Assignments,
					&proto.ShardAssignment{
						Shard:  shard,
						Leader: leader,
						ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
							Int32HashRange: &proto.Int32HashRange{
								MinHashInclusive: a.Int32HashRange.Min,
								MaxHashInclusive: a.Int32HashRange.Max,
							},
						},
					},
				)
			}
		}

		c.assignments.Namespaces[name] = nsAssignments
	}

	c.assignmentsChanged.Broadcast()
}

func (c *coordinator) ClusterStatus() model.ClusterStatus {
	c.Lock()
	defer c.Unlock()
	return *c.clusterStatus.Clone()
}

func (c *coordinator) startBackgroundActionWorker() {
	for {
		select {
		case action := <-c.loadBalancer.Action():
			switch action.Type() { //nolint:revive,gocritic
			case balancer.SwapNode:
				c.handleActionSwap(action)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *coordinator) handleActionSwap(action balancer.Action) {
	var ac *balancer.SwapNodeAction
	var ok bool
	if ac, ok = action.(*balancer.SwapNodeAction); !ok {
		panic("unexpected action type")
	}
	defer ac.Done()
	c.log.Info("Applying swap action", slog.Any("swap-action", ac))

	c.Lock()
	sc, ok := c.shardControllers[ac.Shard]
	c.Unlock()
	if !ok {
		c.log.Warn("Shard controller not found", slog.Int64("shard", ac.Shard))
		return
	}
	index := c.ServerIDIndex()
	drainingIndex := c.DrainingServerIDIndex()
	var fromServer *model.Server
	var exist bool
	if fromServer, exist = index[ac.From]; !exist {
		fromServer = drainingIndex[ac.From]
	}
	toServer := index[ac.To]

	if fromServer == nil || toServer == nil {
		c.log.Warn("server not found for swap, skipped.", slog.String("from", ac.From), slog.String("to", ac.To))
		return
	}
	// todo: use pointer here
	if err := sc.SwapNode(*fromServer, *toServer); err != nil {
		c.log.Warn("Failed to swap node", slog.Any("error", err), slog.Any("swap-action", ac))
	}
}

func (c *coordinator) waitForExternalEvents() {
	for {
		select {
		case <-c.ctx.Done():
			return

		case <-c.clusterConfigChangeCh:
			c.log.Info("Received cluster config change event")
			if err := c.handleClusterConfigUpdated(); err != nil {
				c.log.Warn("Failed to update cluster config", slog.Any("error", err))
			}
			c.loadBalancer.Trigger()
		}
	}
}

func (c *coordinator) handleClusterConfigUpdated() error {
	c.Lock()
	defer c.Unlock()

	newClusterConfig, err := c.clusterConfigProvider()
	if err != nil {
		return errors.Wrap(err, "failed to read cluster configuration")
	}
	if reflect.DeepEqual(newClusterConfig, c.ClusterConfig) {
		c.log.Info("No cluster config changes detected")
		return nil
	}
	c.log.Info(
		"Detected change in cluster config",
		slog.Any("clusterConfig", c.ClusterConfig),
		slog.Any("newClusterConfig", newClusterConfig),
		slog.Any("metadataVersion", c.metadataVersion),
	)
	c.ClusterConfig = newClusterConfig
	c.serverIndexesOnce = sync.Once{}
	c.checkClusterNodeChanges()
	for _, sc := range c.shardControllers {
		sc.SyncServerAddress()
	}
	clusterStatus, shardsToAdd, shardsToDelete := applyClusterChanges(&newClusterConfig, c.clusterStatus, c.selectNewEnsemble)
	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		namespaceConfig := GetNamespaceConfig(c.Namespaces, namespace)
		c.shardControllers[shard] = NewShardController(namespace, shard, namespaceConfig, shardMetadata, c.rpc, c)
		slog.Info("Added new shard", slog.Int64("shard", shard),
			slog.String("namespace", namespace), slog.Any("shard-metadata", shardMetadata))
	}
	for _, shard := range shardsToDelete {
		s, ok := c.shardControllers[shard]
		if ok {
			s.DeleteShard()
		}
	}
	c.clusterStatus = clusterStatus
	c.computeNewAssignments()
	return nil
}

func (c *coordinator) FindServerByIdentifier(identifier string) (*model.Server, bool) {
	server, exist := c.ServerIDIndex()[identifier]
	return server, exist
}

func (c *coordinator) checkClusterNodeChanges() {
	// Check for nodes to add
	for _, sa := range c.Servers {
		if _, ok := c.nodeControllers[sa.GetIdentifier()]; ok {
			continue
		}

		// The node is present in the config, though we don't know it yet,
		// therefore it must be a newly added node
		c.log.Info("Detected new node", slog.Any("server", sa))
		if nc, ok := c.drainingNodes[sa.GetIdentifier()]; ok {
			// If there were any controller for a draining node, close it
			// and recreate it as a new node
			_ = nc.Close()
			delete(c.drainingNodes, sa.GetIdentifier())
		}
		c.nodeControllers[sa.GetIdentifier()] = NewNodeController(sa, c, c, c.rpc)
	}

	// Check for nodes to remove
	for serverID, nc := range c.nodeControllers {
		if _, exist := c.FindServerByIdentifier(serverID); exist {
			continue
		}

		c.log.Info("Detected a removed node", slog.Any("server", serverID))
		// Moved the node
		delete(c.nodeControllers, serverID)
		nc.SetStatus(Draining)
		c.drainingNodes[serverID] = nc
	}
}

func (c *coordinator) getNodeControllers() map[string]NodeController {
	c.Lock()
	defer c.Unlock()
	nc := make(map[string]NodeController)
	for k, v := range c.nodeControllers {
		nc[k] = v
	}
	return nc
}

func GetNamespaceConfig(namespaces []model.NamespaceConfig, namespace string) *model.NamespaceConfig {
	for _, nc := range namespaces {
		if nc.Name == namespace {
			return &nc
		}
	}

	return &model.NamespaceConfig{}
}
