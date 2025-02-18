package ensemble

import "github.com/streamnative/oxia/coordinator/model"

type Selector interface {
	Select() []model.NodeInfo
}
