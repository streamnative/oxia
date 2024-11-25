package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClientPool_GetActualAddress(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	address := poolInstance.getActualAddress("tls://xxxxaa:6648")
	assert.Equal(t, "xxxxaa:6648", address)

	actualAddress := poolInstance.getActualAddress("xxxxaaa:6649")
	assert.Equal(t, "xxxxaaa:6649", actualAddress)
}

func TestClientPool_GetTransportCredential(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	credential := poolInstance.getTransportCredential("tls://xxxxaa:6648")
	assert.Equal(t, "tls", credential.Info().SecurityProtocol)

	credential = poolInstance.getTransportCredential("xxxxaaa:6649")
	assert.Equal(t, "insecure", credential.Info().SecurityProtocol)
}
