package ensemble

import (
	"github.com/streamnative/oxia/coordinator/model"
	"sync"
)

type PolicySelector struct {
	nodes sync.Map
}

func (l *PolicySelector) Select() []model.NodeInfo {

}
