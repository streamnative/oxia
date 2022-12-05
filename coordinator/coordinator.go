package coordinator

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"
	"io"
	"math"
	"oxia/common"
	"oxia/proto"
	"sync"
)

type ShardAssignmentsProvider interface {
	WaitForNextUpdate(currentValue *proto.ShardAssignmentsResponse) *proto.ShardAssignmentsResponse
}

type Coordinator interface {
	io.Closer

	ShardAssignmentsProvider

	InitiateLeaderElection(shard uint32, newEpoch int64) error
	ElectedLeader(shard uint32, epoch int64, leader ServerAddress) error
}

type coordinator struct {
	sync.Mutex
	assignmentsChanged *sync.Cond

	MetadataProvider
	ClusterConfig

	shardControllers map[uint32]ShardController
	nodeControllers  map[string]NodeController
	clusterStatus    *ClusterStatus
	assignments      *proto.ShardAssignmentsResponse
	metadataVersion  int64
	clientPool       common.ClientPool
	log              zerolog.Logger
}

func NewCoordinator(metadataProvider MetadataProvider, clusterConfig ClusterConfig) (Coordinator, error) {
	c := &coordinator{
		MetadataProvider: metadataProvider,
		ClusterConfig:    clusterConfig,
		shardControllers: make(map[uint32]ShardController),
		nodeControllers:  make(map[string]NodeController),
		clientPool:       common.NewClientPool(),
		log: log.With().
			Str("component", "coordinator").
			Logger(),
	}

	c.assignmentsChanged = sync.NewCond(c)

	var err error
	c.clusterStatus, c.metadataVersion, err = metadataProvider.Get()
	if err != nil && !errors.Is(err, ErrorMetadataNotInitialized) {
		return nil, err
	}

	if c.clusterStatus == nil {
		if err = c.initialAssignment(); err != nil {
			return nil, err
		}
	}

	for _, sa := range clusterConfig.StorageServers {
		c.nodeControllers[sa.Internal] = NewNodeController(sa.Internal, c, c.clientPool)
	}

	return c, nil
}

// Assign the shards to the available servers
func (c *coordinator) initialAssignment() error {
	c.log.Info().
		Interface("clusterConfig", c.ClusterConfig).
		Msg("Performing initial assignment")

	cc := c.ClusterConfig
	cs := &ClusterStatus{
		ReplicationFactor: cc.ReplicationFactor,
		Shards:            make(map[uint32]*ShardMetadata),
	}

	bucketSize := math.MaxUint32 / cc.ShardsCount

	// Do round-robin assignment of shards to storage servers
	serverIdx := uint32(0)

	for i := uint32(0); i < cc.ShardsCount; i++ {
		cs.Shards[i] = &ShardMetadata{
			Status:   ShardStatusUnknown,
			Epoch:    -1,
			Leader:   nil,
			Ensemble: getServers(cc.StorageServers, serverIdx, cc.ReplicationFactor),
			Int32HashRange: Int32HashRange{
				Min: bucketSize * i,
				Max: bucketSize*(i+1) - 1,
			},
		}

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

	for shard, shardMetadata := range c.clusterStatus.Shards {
		c.shardControllers[shard] = NewShardController(shard, shardMetadata, c.clientPool, c)
	}

	return nil
}

func getServers(servers []ServerAddress, startIdx uint32, count uint32) []ServerAddress {
	n := len(servers)
	res := make([]ServerAddress, count)
	for i := uint32(0); i < count; i++ {
		res[i] = servers[int(startIdx+i)%n]
	}
	return res
}

func (c *coordinator) Close() error {
	err := c.clientPool.Close()

	for _, sc := range c.shardControllers {
		err = multierr.Append(err, sc.Close())
	}

	for _, nc := range c.nodeControllers {
		err = multierr.Append(err, nc.Close())
	}
	return err
}

func (c *coordinator) WaitForNextUpdate(currentValue *proto.ShardAssignmentsResponse) *proto.ShardAssignmentsResponse {
	c.Lock()
	defer c.Unlock()

	for pb.Equal(currentValue, c.assignments) {
		// Wait on the condition until the assignments get changed
		c.assignmentsChanged.Wait()
	}

	return c.assignments
}

func (c *coordinator) InitiateLeaderElection(shard uint32, newEpoch int64) error {
	c.Lock()
	defer c.Unlock()

	cs := c.clusterStatus.Clone()
	sm := cs.Shards[shard]
	sm.Status = ShardStatusElection
	sm.Epoch = newEpoch

	newMetadataVersion, err := c.MetadataProvider.Store(cs, c.metadataVersion)
	if err != nil {
		return err
	}

	c.metadataVersion = newMetadataVersion
	return nil
}

func (c *coordinator) ElectedLeader(shard uint32, epoch int64, leader ServerAddress) error {
	c.Lock()
	defer c.Unlock()

	cs := c.clusterStatus.Clone()
	sm := cs.Shards[shard]
	sm.Status = ShardStatusSteadyState
	sm.Epoch = epoch
	sm.Leader = &leader
	cs.Shards[shard] = sm

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
	c.assignments = &proto.ShardAssignmentsResponse{
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
						MaxHashExclusive: a.Int32HashRange.Max,
					},
				},
			},
		)
	}

	c.assignmentsChanged.Broadcast()
}
