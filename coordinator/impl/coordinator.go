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
	"sync"
	"time"
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

	InitiateLeaderElection(shard uint32, metadata model.ShardMetadata) error
	ElectedLeader(shard uint32, metadata model.ShardMetadata) error

	NodeAvailabilityListener

	ClusterStatus() model.ClusterStatus
}

type coordinator struct {
	sync.Mutex
	assignmentsChanged common.ConditionContext

	MetadataProvider
	model.ClusterConfig

	shardControllers map[uint32]ShardController
	nodeControllers  map[string]NodeController
	clusterStatus    *model.ClusterStatus
	assignments      *proto.ShardAssignments
	metadataVersion  Version
	rpc              RpcProvider
	log              zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

func NewCoordinator(metadataProvider MetadataProvider, clusterConfig model.ClusterConfig, rpc RpcProvider) (Coordinator, error) {
	c := &coordinator{
		MetadataProvider: metadataProvider,
		ClusterConfig:    clusterConfig,
		shardControllers: make(map[uint32]ShardController),
		nodeControllers:  make(map[string]NodeController),
		rpc:              rpc,
		log: log.With().
			Str("component", "coordinator").
			Logger(),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.assignmentsChanged = common.NewConditionContext(c)

	var err error
	c.clusterStatus, c.metadataVersion, err = metadataProvider.Get()
	if err != nil && !errors.Is(err, ErrorMetadataNotInitialized) {
		return nil, err
	}

	for _, sa := range clusterConfig.Servers {
		c.nodeControllers[sa.Internal] = NewNodeController(sa, c, c, c.rpc)
	}

	// Before initializing the cluster, it's better to make sure we
	// have all the nodes available, otherwise the coordinator might be
	// the first component in getting started and will print out a lot
	// of error logs regarding failed leader elections
	c.waitForAllNodesToBeAvailable()

	if c.clusterStatus == nil {
		if err = c.initialAssignment(); err != nil {
			return nil, err
		}
	}

	for shard, shardMetadata := range c.clusterStatus.Shards {
		c.shardControllers[shard] = NewShardController(shard, shardMetadata, c.rpc, c)
	}

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

	cc := c.ClusterConfig
	cs := &model.ClusterStatus{
		ReplicationFactor: cc.ReplicationFactor,
		Shards:            make(map[uint32]model.ShardMetadata),
	}

	shards := common.GenerateShards(cc.InitialShardCount)

	// Do round-robin assignment of shards to storage servers
	serverIdx := uint32(0)

	for i, shard := range shards {
		shardMetadata := model.ShardMetadata{
			Status:   model.ShardStatusUnknown,
			Term:     -1,
			Leader:   nil,
			Ensemble: getServers(cc.Servers, serverIdx, cc.ReplicationFactor),
			Int32HashRange: model.Int32HashRange{
				Min: shard.Min,
				Max: shard.Max,
			},
		}

		cs.Shards[uint32(i)] = shardMetadata
		serverIdx += cc.ReplicationFactor
	}

	c.log.Info().
		Interface("cluster-status", cs).
		Msg("Initializing cluster status")

	var err error
	if c.metadataVersion, err = c.MetadataProvider.Store(cs, MetadataNotExists); err != nil {
		return err
	}

	c.clusterStatus = cs

	return nil
}

func getServers(servers []model.ServerAddress, startIdx uint32, count uint32) []model.ServerAddress {
	n := len(servers)
	res := make([]model.ServerAddress, count)
	for i := uint32(0); i < count; i++ {
		res[i] = servers[int(startIdx+i)%n]
	}
	return res
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
	defer c.Unlock()

	for _, sc := range c.shardControllers {
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

func (c *coordinator) InitiateLeaderElection(shard uint32, metadata model.ShardMetadata) error {
	c.Lock()
	defer c.Unlock()

	cs := c.clusterStatus.Clone()
	cs.Shards[shard] = metadata

	newMetadataVersion, err := c.MetadataProvider.Store(cs, c.metadataVersion)
	if err != nil {
		return err
	}

	c.metadataVersion = newMetadataVersion
	return nil
}

func (c *coordinator) ElectedLeader(shard uint32, metadata model.ShardMetadata) error {
	c.Lock()
	defer c.Unlock()

	cs := c.clusterStatus.Clone()
	cs.Shards[shard] = metadata

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
		Assignments:    make([]*proto.ShardAssignment, 0),
		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
	}

	// Update the leader for the shards
	for shard, a := range c.clusterStatus.Shards {
		var leader string
		if a.Leader != nil {
			leader = a.Leader.Public
		}
		c.assignments.Assignments = append(c.assignments.Assignments,
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

	c.assignmentsChanged.Broadcast()
}

func (c *coordinator) ClusterStatus() model.ClusterStatus {
	c.Lock()
	defer c.Unlock()
	return *c.clusterStatus.Clone()
}
