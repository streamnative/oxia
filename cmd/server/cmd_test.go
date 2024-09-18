package server

import (
	"github.com/spf13/cobra"
	"github.com/streamnative/oxia/server"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestServerCmd(t *testing.T) {
	for _, test := range []struct {
		args         []string
		expectedConf server.Config
		isErr        bool
	}{
		{[]string{}, server.Config{
			PublicServiceAddr:          "0.0.0.0:6648",
			InternalServiceAddr:        "0.0.0.0:6649",
			MetricsServiceAddr:         "0.0.0.0:8080",
			DataDir:                    "./data/db",
			WalDir:                     "./data/wal",
			WalRetentionTime:           1 * time.Hour,
			WalSyncData:                true,
			NotificationsRetentionTime: 1 * time.Hour,
			DbBlockCacheMB:             100,
		}, false},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			Cmd.SetArgs(test.args)
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				return nil
			}
			err := Cmd.Execute()
			assert.Equal(t, test.isErr, err != nil)
			assert.Equal(t, test.expectedConf, conf)
		})
	}
}
