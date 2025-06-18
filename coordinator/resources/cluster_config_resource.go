package resources

import (
	"context"
	"io"
	"log/slog"
	"sync"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/emirpasic/gods/v2/trees/redblacktree"

	"github.com/streamnative/oxia/common/process"
	"github.com/streamnative/oxia/coordinator/model"
)

type ClusterConfigResource interface {
	io.Closer

	Load() *model.ClusterConfig

	Nodes() *linkedhashset.Set[string]

	NodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata)

	NamespaceConfig(namespace string) (*model.NamespaceConfig, bool)

	Node(id string) (*model.Server, bool)
}

var _ ClusterConfigResource = &clusterConfig{}

type clusterConfig struct {
	*slog.Logger
	sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	clusterConfigProvider        func() (model.ClusterConfig, error)
	clusterConfigNotificationsCh chan any
	clusterConfigEventListener   ClusterConfigEventListener

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *model.ClusterConfig
	nodesIndex            *redblacktree.Tree[string, *model.Server]
	namespaceConfigsIndex *redblacktree.Tree[string, *model.NamespaceConfig]
}

func (ccf *clusterConfig) loadWithInitSlow() {
	ccf.clusterConfigLock.Lock()
	defer ccf.clusterConfigLock.Unlock()
	if ccf.currentClusterConfig != nil {
		return
	}
	newConfig, err := ccf.clusterConfigProvider()
	if err != nil {
		panic(err)
	}
	ccf.currentClusterConfig = &newConfig
	index := redblacktree.New[string, *model.Server]()
	for idx, server := range ccf.currentClusterConfig.Servers {
		index.Put(server.GetIdentifier(), &ccf.currentClusterConfig.Servers[idx])
	}
	ccf.nodesIndex = index
	ncIndex := redblacktree.New[string, *model.NamespaceConfig]()
	for idx, ns := range ccf.currentClusterConfig.Namespaces {
		ncIndex.Put(ns.Name, &ccf.currentClusterConfig.Namespaces[idx])
	}
	ccf.namespaceConfigsIndex = ncIndex
}

func (ccf *clusterConfig) Close() error {
	ccf.cancel()
	ccf.Wait()
	return nil
}

func (ccf *clusterConfig) Load() *model.ClusterConfig {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		defer ccf.clusterConfigLock.RLock()
		ccf.loadWithInitSlow()
	}
	return ccf.currentClusterConfig
}

func (ccf *clusterConfig) Nodes() *linkedhashset.Set[string] {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		defer ccf.clusterConfigLock.RLock()
		ccf.loadWithInitSlow()
	}
	nodes := linkedhashset.New[string]()
	for idx := range ccf.currentClusterConfig.Servers {
		nodes.Add(ccf.currentClusterConfig.Servers[idx].GetIdentifier())
	}
	return nodes
}

func (ccf *clusterConfig) NodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata) {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		defer ccf.clusterConfigLock.RLock()
		ccf.loadWithInitSlow()
	}
	nodes := linkedhashset.New[string]()
	for idx := range ccf.currentClusterConfig.Servers {
		nodes.Add(ccf.currentClusterConfig.Servers[idx].GetIdentifier())
	}
	metadata := ccf.currentClusterConfig.ServerMetadata
	return nodes, metadata
}

func (ccf *clusterConfig) NamespaceConfig(namespace string) (*model.NamespaceConfig, bool) {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		defer ccf.clusterConfigLock.RLock()
		ccf.loadWithInitSlow()
	}
	return ccf.namespaceConfigsIndex.Get(namespace)
}

func (ccf *clusterConfig) Node(id string) (*model.Server, bool) {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		defer ccf.clusterConfigLock.RLock()
		ccf.loadWithInitSlow()
	}
	return ccf.nodesIndex.Get(id)
}

func (ccf *clusterConfig) waitForUpdates() {
	for {
		select {
		case <-ccf.ctx.Done():
			return

		case <-ccf.clusterConfigNotificationsCh:
			ccf.Info("Received cluster config change event")
			ccf.clusterConfigLock.Lock()
			ccf.currentClusterConfig = nil
			ccf.clusterConfigLock.Unlock()

			ccf.loadWithInitSlow()

			ccf.clusterConfigLock.RLock()
			currentClusterConfig := ccf.currentClusterConfig
			ccf.clusterConfigLock.RUnlock()

			ccf.clusterConfigEventListener.ConfigChanged(currentClusterConfig)
		}
	}
}

func NewClusterConfigResource(ctx context.Context,
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigNotificationsCh chan any,
	clusterConfigEventListener ClusterConfigEventListener,
) ClusterConfigResource {
	ctx, cancelFunc := context.WithCancel(ctx)

	cc := &clusterConfig{
		ctx:                          ctx,
		cancel:                       cancelFunc,
		clusterConfigProvider:        clusterConfigProvider,
		clusterConfigNotificationsCh: clusterConfigNotificationsCh,
		clusterConfigEventListener:   clusterConfigEventListener,
	}

	go process.DoWithLabels(ctx, map[string]string{
		"component": "coordinator-action-worker",
	}, cc.waitForUpdates)

	return cc
}
