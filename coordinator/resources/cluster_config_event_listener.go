package resources

import "github.com/streamnative/oxia/coordinator/model"

type ClusterConfigEventListener interface {
	ConfigChanged(newConfig *model.ClusterConfig)
}
