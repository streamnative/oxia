package ensemble

import (
	"testing"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestLastAllocator(t *testing.T) {
	allocator := &lastAllocator{}

	server1 := model.Server{Name: ptr.To("server1"), Public: "server1", Internal: "server1"}
	server2 := model.Server{Name: ptr.To("server2"), Public: "server2", Internal: "server2"}
	server3 := model.Server{Name: ptr.To("server3"), Public: "server3", Internal: "server3"}
	server4 := model.Server{Name: ptr.To("server4"), Public: "server4", Internal: "server4"}
	server5 := model.Server{Name: ptr.To("server5"), Public: "server5", Internal: "server5"}
	server6 := model.Server{Name: ptr.To("server6"), Public: "server6", Internal: "server6"}
	candidates := []model.Server{server1, server2, server3, server4, server5, server6}
	replicas := uint32(6)

	result, err := allocator.AllocateNew(candidates, make(map[string]model.ServerMetadata), nil, nil, replicas)
	assert.NoError(t, err)
	assert.Equal(t, result[0], server1)
	assert.Equal(t, result[1], server2)
	assert.Equal(t, result[2], server3)
}
