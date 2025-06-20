package balancer

import (
	"testing"
	"time"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/utils"
	"github.com/oxia-db/oxia/tests/mock"
	"github.com/stretchr/testify/assert"
)

func TestLeaderBalanced(t *testing.T) {
	s1, s1ad := mock.NewServer(t, "sv-1")
	defer s1.Close()
	s2, s2ad := mock.NewServer(t, "sv-2")
	defer s2.Close()
	s3, s3ad := mock.NewServer(t, "sv-3")
	defer s3.Close()

	cc := &model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 5,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 4,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-3",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
		},
		Servers: []model.Server{s1ad, s2ad, s3ad},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()

	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		if len(status.Namespaces) != 3 {
			return false
		}
		for _, ns := range status.Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	balancer := coordinator.LoadBalancer()
	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		shardLeaders, electedShards, nodeShards := utils.NodeShardLeaders(status)
		if shardLeaders != electedShards {
			return false
		}
		balancer.Trigger()
		for _, shards := range nodeShards {
			if len(shards) != 4 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
}
