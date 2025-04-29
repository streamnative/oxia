package balancer

import (
	"context"
	"testing"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
)

func TestLoadBalancerActions(t *testing.T) {
	status := &model.ClusterStatus{}
	serverIds := linkedhashset.New()
	namespaceConfigs := map[string]model.NamespaceConfig{}

	balancer := NewLoadBalancer(Options{
		Context: context.Background(),
		MetadataSupplier: func() map[string]model.ServerMetadata {
			return make(map[string]model.ServerMetadata)
		},
		ClusterServerIdsSupplier: func() *linkedhashset.Set {
			return serverIds
		},
		ClusterStatusSupplier: func() *model.ClusterStatus { return status },
		NamespaceConfigSupplier: func(namespace string) *model.NamespaceConfig {
		},
	})

	<-balancer.Action()
}
