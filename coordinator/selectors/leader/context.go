package leader

import (
	"github.com/oxia-db/oxia/coordinator/model"
)

type Context struct {
	Candidates []model.Server
	Status     *model.ClusterStatus
}
