package mock

import (
	"testing"

	"github.com/streamnative/oxia/common/rpc"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/stretchr/testify/assert"
)

func NewCoordinator(t *testing.T, config *model.ClusterConfig, clusterConfigNotificationCh chan any) impl.Coordinator {
	t.Helper()
	metadataProvider := impl.NewMetadataProviderMemory()
	clientPool := rpc.NewClientPool(nil, nil)
	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return *config, nil }, clusterConfigNotificationCh, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	return coordinator
}
