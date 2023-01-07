package standalone

import (
	"github.com/spf13/cobra"
	"io"
	"oxia/cmd/flag"
	"oxia/common"
	"oxia/server"
	"time"
)

var (
	conf = server.StandaloneConfig{}

	Cmd = &cobra.Command{
		Use:   "standalone",
		Short: "Start a standalone service",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	flag.PublicPort(Cmd, &conf.PublicServicePort)
	flag.MetricsPort(Cmd, &conf.MetricsPort)
	Cmd.Flags().StringVarP(&conf.AdvertisedPublicAddress, "advertised-address", "a", "", "Advertised address")
	Cmd.Flags().Uint32VarP(&conf.NumShards, "shards", "s", 1, "Number of shards")
	Cmd.Flags().StringVar(&conf.DataDir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().StringVar(&conf.WalDir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
	Cmd.Flags().DurationVar(&conf.WalRetentionTime, "wal-retention-time", 1*time.Hour, "Retention time for the entries in the write-ahead-log")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return server.NewStandalone(conf)
	})
}
