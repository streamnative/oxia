package single

import (
	"github.com/streamnative/oxia/coordinator/selectors"
	"k8s.io/apimachinery/pkg/util/rand"
)

var _ selectors.Selector[*Context, string] = &finalSelector{}

type finalSelector struct{}

func (f finalSelector) Select(ssContext *Context) (string, error) {
	status := ssContext.Status
	candidatesArr := ssContext.Candidates.Values()
	if len(candidatesArr) == 0 {
		return "", selectors.ErrNoFunctioning
	}
	if status != nil {
		startIdx := ssContext.Status.ServerIdx
		return candidatesArr[int(startIdx)%len(candidatesArr)].(string), nil
	}
	return candidatesArr[rand.Intn(len(candidatesArr))].(string), nil
}
