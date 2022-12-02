package server

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
)

func TestNewServer(t *testing.T) {
	config := Config{
		InternalServicePort: 0,
		PublicServicePort:   0,
		MetricsPort:         0,
	}

	server, err := New(config)
	assert.NoError(t, err)

	url := fmt.Sprintf("http://localhost:%d/metrics", server.metrics.Port())
	response, err := http.Get(url)
	assert.NoError(t, err)

	assert.Equal(t, 200, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	assert.NoError(t, err)

	// Looks like exposition format
	assert.Equal(t, "# HELP ", string(body[0:7]))
}
