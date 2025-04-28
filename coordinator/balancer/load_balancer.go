package balancer

import (
	"io"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
	"golang.org/x/net/context"
)

type LoadRatioAlgorithm = func(params *LoadRatioParams) *LoadRatio

type Options struct {
	context.Context
	actionCh chan<- Action

	clusterStatusSupplier    func() *model.ClusterStatus
	namespaceConfigSupplier  func(namespace string) *model.NamespaceConfig
	metadataSupplier         func() map[string]model.ServerMetadata
	clusterServerIdsSupplier func() *linkedhashset.Set
}

type LoadBalancer interface {
	io.Closer
}
