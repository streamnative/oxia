package resources

import "github.com/oxia-db/oxia/coordinator/model"

type ClusterConfigEventListener interface {
	ConfigChanged(newConfig *model.ClusterConfig)
}
