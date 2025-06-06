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

	cc := model.ClusterConfig{
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
		Servers: []model.Server{s1ad, s2ad, s3ad},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, &cc, ch)
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

	cc.Servers = append(cc.Servers, s4ad, s5ad)
	ch <- struct{}{}

	assert.Eventually(t, func() bool {
		_, exist := coordinator.FindServerByIdentifier(s4ad.GetIdentifier())
		return exist
	}, 10*time.Second, 50*time.Millisecond)

	assert.Eventually(t, func() bool {
		coordinator.TriggerBalance()
		return coordinator.IsBalanced()
	}, 30*time.Second, 50*time.Millisecond)
}

func TestPolicyBasedShardBalancer(t *testing.T) {

}
