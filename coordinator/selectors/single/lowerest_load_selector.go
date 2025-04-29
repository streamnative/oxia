package single

import (
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/selectors"
)

var _ selectors.Selector[*Context, *string] = &lowerestLoadSelector{}

type lowerestLoadSelector struct{}

func (l *lowerestLoadSelector) Select(ssContext *Context) (*string, error) {
	if ssContext.LoadRatioSupplier == nil {
		return nil, selectors.ErrNoFunctioning
	}
	loadRatios := ssContext.LoadRatioSupplier()
	if loadRatios == nil {
		return nil, selectors.ErrNoFunctioning
	}
	iter := loadRatios.NodeLoadRatios().Iterator()
	iter.Last()
	for iter.Prev() {
		nodeRatio := iter.Value().(*model.NodeLoadRatio)
		lowerLoad := nodeRatio.NodeID
		if ssContext.Candidates.Contains(lowerLoad) {
			return &lowerLoad, nil
		}
	}
	return nil, selectors.ErrNoFunctioning
}
