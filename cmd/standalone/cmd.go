package standalone

import (
	"github.com/spf13/cobra"
	"io"
	"oxia/common"
	"oxia/standalone"
)

var (
	conf = standalone.Config{}

	Cmd = &cobra.Command{
		Use:   "standalone",
		Short: "Start a standalone service",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	Cmd.Flags().Uint32VarP(&conf.PublicServicePort, "port", "p", 9190, "Public service port")
	Cmd.Flags().IntVarP(&conf.MetricsPort, "metrics-port", "m", 8080, "Metrics port")
	Cmd.Flags().StringVarP(&conf.AdvertisedPublicAddress, "advertised-address", "a", "", "Advertised address")
	Cmd.Flags().Uint32VarP(&conf.NumShards, "shards", "s", 1, "Number of shards")
	Cmd.Flags().StringVar(&conf.DataDir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().StringVar(&conf.WalDir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return standalone.New(conf)
	})
}
