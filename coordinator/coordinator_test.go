package coordinator

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCoordinator_MarshalingError(t *testing.T) {
	config := Config{
		InternalServiceAddr:       "123",
		InternalSecureServiceAddr: "123",
		MetadataProviderImpl:      "123",
		K8SMetadataConfigMapName:  "123",
	}
	_, err := json.Marshal(config)
	assert.Nil(t, err)
}
