package metrics

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
)

func TestPrometheusMetrics(t *testing.T) {
	metrics, err := Start("localhost:0")
	assert.NoError(t, err)

	url := fmt.Sprintf("http://localhost:%d/metrics", metrics.Port())
	response, err := http.Get(url)
	assert.NoError(t, err)

	assert.Equal(t, 200, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	assert.NoError(t, err)

	// Looks like exposition format
	assert.Equal(t, "# HELP ", string(body[0:7]))

	err = metrics.Close()
	assert.NoError(t, err)

	response, err = http.Get(url)
	assert.ErrorContains(t, err, "connection refused")
	assert.Nil(t, response)
}
