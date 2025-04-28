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

	ClusterStatusSupplier    func() *model.ClusterStatus
	NamespaceConfigSupplier  func(namespace string) *model.NamespaceConfig
	MetadataSupplier         func() map[string]model.ServerMetadata
	ClusterServerIdsSupplier func() *linkedhashset.Set
}

type LoadBalancer interface {
	io.Closer

	Action() <-chan Action
}
