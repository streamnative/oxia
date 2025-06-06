package balancer

import (
	"testing"
	"time"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/tests/mock"
	"github.com/stretchr/testify/assert"
)

func TestNormalShardBalancer(t *testing.T) {
	s1, s1ad := mock.NewServer(t)
	defer s1.Close()
	s2, s2ad := mock.NewServer(t)
	defer s2.Close()
	s3, s3ad := mock.NewServer(t)
	defer s3.Close()
	s4, s4ad := mock.NewServer(t)
	defer s4.Close()
	s5, s5ad := mock.NewServer(t)
	defer s5.Close()

	servers := []model.Server{s1ad, s2ad, s3ad}

	coordinator := mock.NewCoordinator(t, model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-3",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
		},
		Servers: servers,
	})
	defer coordinator.Close()

	assert.Eventually(t, func() bool {
		for _, ns := range coordinator.ClusterStatus().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	servers = append(servers, s4ad, s5ad)

}

func TestPolicyBasedShardBalancer(t *testing.T) {

}
