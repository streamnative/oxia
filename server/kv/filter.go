package kv

import (
	"github.com/streamnative/oxia/common"
	"strings"
)

type Filter func(key string) bool

func DisableFilter(key string) bool {
	return false
}

func InternalKeyFilter(key string) bool {
	return strings.HasPrefix(key, common.InternalKeyPrefix)
}
