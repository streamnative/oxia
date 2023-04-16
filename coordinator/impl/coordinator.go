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
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"
	"io"
	"oxia/common"
	"oxia/coordinator/model"
	"oxia/proto"
	"reflect"
	"sync"
	"time"
)

var (
	ErrorNamespaceNotFound = errors.New("namespace not found")
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
	clusterConfigRefreshTime time.Duration

	shardControllers map[int64]ShardController
	nodeControllers  map[string]NodeController
	clusterStatus    *model.ClusterStatus
	assignments      *proto.ShardAssignments
	metadataVersion  Version
	rpc              RpcProvider
	log              zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

func NewCoordinator(metadataProvider MetadataProvider,
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigRefreshTime time.Duration,
	rpc RpcProvider) (Coordinator, error) {

	initialClusterConf, err := clusterConfigProvider()
	if err != nil {
		return nil, err
	}

	if clusterConfigRefreshTime == 0 {
		clusterConfigRefreshTime = 1 * time.Minute
	}

	c := &coordinator{
		MetadataProvider:         metadataProvider,
		clusterConfigProvider:    clusterConfigProvider,
		ClusterConfig:            initialClusterConf,
		clusterConfigRefreshTime: clusterConfigRefreshTime,
		shardControllers:         make(map[int64]ShardController),
		nodeControllers:          make(map[string]NodeController),
		rpc:                      rpc,
		log: log.With().
			Str("component", "coordinator").
			Logger(),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.assignmentsChanged = common.NewConditionContext(c)

	c.clusterStatus, c.metadataVersion, err = metadataProvider.Get()
	if err != nil && !errors.Is(err, ErrorMetadataNotInitialized) {
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

	for ns, shards := range c.clusterStatus.Namespaces {
		for shard, shardMetadata := range shards.Shards {
			c.shardControllers[shard] = NewShardController(ns, shard, shardMetadata, c.rpc, c)
		}
	}

	go common.DoWithLabels(map[string]string{
		"oxia": "coordinator-wait-for-events",
	}, c.waitForExternalEvents)

	return c, nil
}

func (c *coordinator) waitForAllNodesToBeAvailable() {
	c.log.Info().Msg("Waiting for all the nodes to be available")
	for {

		select {

		case <-time.After(1 * time.Second):
			allNodesAvailable := true
			for _, n := range c.nodeControllers {
				if n.Status() != Running {
					allNodesAvailable = false
				}
			}
			if allNodesAvailable {
				c.log.Info().Msg("All nodes are now available")
				return
			}

		case <-c.ctx.Done():
			// Give up if we're closing the coordinator
			return
		}
	}
}

// Assign the shards to the available servers
func (c *coordinator) initialAssignment() error {
	c.log.Info().
		Interface("clusterConfig", c.ClusterConfig).
		Msg("Performing initial assignment")

	clusterStatus, _, _ := applyClusterChanges(&c.ClusterConfig, model.NewClusterStatus())

	var err error
	if c.metadataVersion, err = c.MetadataProvider.Store(clusterStatus, MetadataNotExists); err != nil {
		return err
	}

	c.clusterStatus = clusterStatus
	return nil
}

func (c *coordinator) applyNewClusterConfig() error {
	c.log.Info().
		Interface("clusterConfig", c.ClusterConfig).
		Interface("metadataVersion", c.metadataVersion).
		Msg("Checking cluster config")

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
	return err
}

func (c *coordinator) NodeBecameUnavailable(node model.ServerAddress) {
	c.Lock()
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
		return ErrorNamespaceNotFound
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
		return ErrorNamespaceNotFound
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
		return ErrorNamespaceNotFound
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

// This is called while already holding the lock on the coordinator
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
						ShardId: shard,
						Leader:  leader,
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
	refreshTimer := time.NewTicker(c.clusterConfigRefreshTime)
	defer refreshTimer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-refreshTimer.C:
			if err := c.handleClusterConfigUpdated(); err != nil {
				c.log.Warn().Err(err).
					Msg("Failed to update cluster config")
			}

			if err := c.rebalanceCluster(); err != nil {
				c.log.Warn().Err(err).
					Msg("Failed to rebalance cluster")
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
		c.log.Debug().
			Msg("Cluster config has not changed since last time")
		return nil
	}

	c.log.Info().
		Interface("clusterConfig", c.ClusterConfig).
		Interface("metadataVersion", c.metadataVersion).
		Msg("Detected change in cluster config")

	clusterStatus, shardsToAdd, shardsToDelete := applyClusterChanges(&newClusterConfig, c.clusterStatus)

	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		c.shardControllers[shard] = NewShardController(namespace, shard, shardMetadata, c.rpc, c)
		log.Info().
			Int64("shard", shard).
			Str("namespace", namespace).
			Interface("shard-metadata", shardMetadata).
			Msg("Added new shard")
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

func (c *coordinator) rebalanceCluster() error {
	c.Lock()
	actions := rebalanceCluster(c.ClusterConfig.Servers, c.clusterStatus)
	c.Unlock()

	for _, swapAction := range actions {
		c.log.Info().
			Interface("swap-action", swapAction).
			Msg("Applying swap action")

		c.Lock()
		sc, ok := c.shardControllers[swapAction.Shard]
		c.Unlock()
		if !ok {
			c.log.Warn().
				Int64("shard", swapAction.Shard).
				Msg("Shard controller not found")
			continue
		}

		if err := sc.SwapNode(swapAction.From, swapAction.To); err != nil {
			c.log.Warn().Err(err).
				Interface("swap-action", swapAction).
				Msg("Failed to swap node")
		}
	}

	return nil
}
