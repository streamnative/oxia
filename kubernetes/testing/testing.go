package testing

import (
	"errors"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/testing"
	"strconv"
)

func ResourceVersionSupport(tracker testing.ObjectTracker) testing.ReactionFunc {
	return func(action testing.Action) (handled bool, ret runtime.Object, err error) {
		ns := action.GetNamespace()
		gvr := action.GetResource()
		switch action := action.(type) {
		case testing.CreateActionImpl:
			objMeta := accessor(action.GetObject())
			objMeta.SetResourceVersion("0")
			return false, action.GetObject(), nil
		case testing.UpdateActionImpl:
			objMeta := accessor(action.GetObject())
			existing, err := tracker.Get(gvr, ns, objMeta.GetName())
			if err != nil {
				return false, action.GetObject(), nil
			}
			existingObjMeta := accessor(existing)
			if objMeta.GetResourceVersion() != existingObjMeta.GetResourceVersion() {
				return true, action.GetObject(), k8sErrors.NewConflict(gvr.GroupResource(), objMeta.GetName(), errors.New("conflict"))
			}
			incrementVersion(objMeta)
			return false, action.GetObject(), nil
		}
		return
	}
}

func accessor(obj runtime.Object) metaV1.Object {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		panic(err)
	}
	return objMeta
}

func incrementVersion(meta metaV1.Object) {
	i, err := strconv.ParseUint(meta.GetResourceVersion(), 10, 64)
	if err != nil {
		panic(err)
	}
	i++
	str := strconv.FormatUint(i, 10)
	meta.SetResourceVersion(str)
}
