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

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/common"
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
	NodeBecameUnavailable(node model.ServerAddress)
}

type Coordinator interface {
	io.Closer

	ShardAssignmentsProvider

	InitiateLeaderElection(namespace string, shard int64, metadata model.ShardMetadata) error
	ElectedLeader(namespace string, shard int64, metadata model.ShardMetadata) error
	ShardDeleted(namespace string, shard int64) error

	NodeAvailabilityListener

	ClusterStatus() model.ClusterStatus
}

type coordinator struct {
	sync.Mutex
	assignmentsChanged common.ConditionContext

	MetadataProvider
	clusterConfigProvider func() (model.ClusterConfig, error)
	model.ClusterConfig
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
		shardControllers:      make(map[int64]ShardController),
		nodeControllers:       make(map[string]NodeController),
		drainingNodes:         make(map[string]NodeController),
		rpc:                   rpc,
		log: slog.With(
			slog.String("component", "coordinator"),
		),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.assignmentsChanged = common.NewConditionContext(c)

	c.clusterStatus, c.metadataVersion, err = metadataProvider.Get()
	if err != nil && !errors.Is(err, ErrMetadataNotInitialized) {
		return nil, err
	}

	for _, sa := range c.ClusterConfig.Servers {
		c.nodeControllers[sa.Internal] = NewNodeController(sa, c, c, c.rpc)
	}

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

	c.initialShardController()

	go common.DoWithLabels(
		c.ctx,
		map[string]string{
			"oxia": "coordinator-wait-for-events",
		},
		c.waitForExternalEvents,
	)

	return c, nil
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

func (c *coordinator) initialShardController() {
	for ns, shards := range c.clusterStatus.Namespaces {
		for shard, shardMetadata := range shards.Shards {
			namespaceConfig := GetNamespaceConfig(c.Namespaces, ns)
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

// Assign the shards to the available servers.
func (c *coordinator) initialAssignment() error {
	c.log.Info(
		"Performing initial assignment",
		slog.Any("clusterConfig", c.ClusterConfig),
	)

	clusterStatus, _, _ := applyClusterChanges(&c.ClusterConfig, model.NewClusterStatus())

	var err error
	if c.metadataVersion, err = c.MetadataProvider.Store(clusterStatus, MetadataNotExists); err != nil {
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

	clusterStatus, shardsToAdd, shardsToDelete := applyClusterChanges(&c.ClusterConfig, c.clusterStatus)

	if len(shardsToAdd) > 0 || len(shardsToDelete) > 0 {
		var err error

		if c.metadataVersion, err = c.MetadataProvider.Store(clusterStatus, c.metadataVersion); err != nil {
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

func (c *coordinator) NodeBecameUnavailable(node model.ServerAddress) {
	c.Lock()

	if nc, ok := c.drainingNodes[node.Internal]; ok {
		// The draining node became unavailable. Let's remove it
		delete(c.drainingNodes, node.Internal)
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

	newMetadataVersion, err := c.MetadataProvider.Store(cs, c.metadataVersion)
	if err != nil {
		return err
	}

	c.metadataVersion = newMetadataVersion
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

	newMetadataVersion, err := c.MetadataProvider.Store(cs, c.metadataVersion)
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

	newMetadataVersion, err := c.MetadataProvider.Store(cs, c.metadataVersion)
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

func (c *coordinator) waitForExternalEvents() {
	for {
		select {
		case <-c.ctx.Done():
			return

		case <-c.clusterConfigChangeCh:
			c.log.Info("Received cluster config change event")
			if err := c.handleClusterConfigUpdated(); err != nil {
				c.log.Warn(
					"Failed to update cluster config",
					slog.Any("error", err),
				)
			}

			if err := c.rebalanceCluster(); err != nil {
				c.log.Warn(
					"Failed to rebalance cluster",
					slog.Any("error", err),
				)
			}
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

	c.checkClusterNodeChanges(newClusterConfig)

	clusterStatus, shardsToAdd, shardsToDelete := applyClusterChanges(&newClusterConfig, c.clusterStatus)

	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]

		namespaceConfig := GetNamespaceConfig(c.Namespaces, namespace)
		c.shardControllers[shard] = NewShardController(namespace, shard, namespaceConfig, shardMetadata, c.rpc, c)
		slog.Info(
			"Added new shard",
			slog.Int64("shard", shard),
			slog.String("namespace", namespace),
			slog.Any("shard-metadata", shardMetadata),
		)
	}

	for _, shard := range shardsToDelete {
		s, ok := c.shardControllers[shard]
		if ok {
			s.DeleteShard()
		}
	}

	c.ClusterConfig = newClusterConfig
	c.clusterStatus = clusterStatus

	c.computeNewAssignments()
	return nil
}

//nolint:unparam
func (c *coordinator) rebalanceCluster() error {
	c.Lock()
	actions := rebalanceCluster(c.ClusterConfig.Servers, c.clusterStatus)
	c.Unlock()

	for _, swapAction := range actions {
		c.log.Info(
			"Applying swap action",
			slog.Any("swap-action", swapAction),
		)

		c.Lock()
		sc, ok := c.shardControllers[swapAction.Shard]
		c.Unlock()
		if !ok {
			c.log.Warn(
				"Shard controller not found",
				slog.Int64("shard", swapAction.Shard),
			)
			continue
		}

		if err := sc.SwapNode(swapAction.From, swapAction.To); err != nil {
			c.log.Warn(
				"Failed to swap node",
				slog.Any("error", err),
				slog.Any("swap-action", swapAction),
			)
		}
	}

	return nil
}

func (*coordinator) findServerByInternalAddress(newClusterConfig model.ClusterConfig, server string) *model.ServerAddress {
	for _, s := range newClusterConfig.Servers {
		if server == s.Internal {
			return &s
		}
	}

	return nil
}

func (c *coordinator) checkClusterNodeChanges(newClusterConfig model.ClusterConfig) {
	// Check for nodes to add
	for _, sa := range newClusterConfig.Servers {
		if _, ok := c.nodeControllers[sa.Internal]; ok {
			continue
		}

		// The node is present in the config, though we don't know it yet,
		// therefore it must be a newly added node
		c.log.Info("Detected new node", slog.Any("addr", sa))
		if nc, ok := c.drainingNodes[sa.Internal]; ok {
			// If there were any controller for a draining node, close it
			// and recreate it as a new node
			_ = nc.Close()
			delete(c.drainingNodes, sa.Internal)
		}
		c.nodeControllers[sa.Internal] = NewNodeController(sa, c, c, c.rpc)
	}

	// Check for nodes to remove
	for ia, nc := range c.nodeControllers {
		if c.findServerByInternalAddress(newClusterConfig, ia) != nil {
			continue
		}

		c.log.Info("Detected a removed node", slog.Any("addr", ia))
		// Moved the node
		delete(c.nodeControllers, ia)
		nc.SetStatus(Draining)
		c.drainingNodes[ia] = nc
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

	// This should never happen since we're going through the same list of namespaces that was already checked
	panic("namespace not found")
}
