package mock

import (
	"fmt"
	"testing"
	"time"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/server"
	"github.com/stretchr/testify/assert"
)

func NewServer(t *testing.T, name string) (s *server.Server, addr model.Server) {
	t.Helper()

	var err error
	s, err = server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})

	assert.NoError(t, err)

	tmp := &name
	addr = model.Server{
		Name:     tmp,
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}
