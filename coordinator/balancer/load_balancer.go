package balancer

import (
	"io"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
	"golang.org/x/net/context"
)

type Options struct {
	context.Context

	MetadataSupplier         func() map[string]model.ServerMetadata
	ClusterStatusSupplier    func() *model.ClusterStatus
	ClusterServerIdsSupplier func() *linkedhashset.Set
	NamespaceConfigSupplier  func(namespace string) *model.NamespaceConfig
}

type LoadBalancer interface {
	io.Closer

	Action() <-chan Action
}
