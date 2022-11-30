package standalone

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"oxia/common"
)

var (
	conf = standaloneConfig{}

	Cmd = &cobra.Command{
		Use:   "standalone",
		Short: "Start a standalone service",
		Long:  `Long description`,
		Run:   main,
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

func main(cmd *cobra.Command, args []string) {
	common.ConfigureLogger()

	server, err := newStandalone(&conf)
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the server")
	}

	profiler := common.RunProfiling()

	common.WaitUntilSignal(
		profiler,
		server,
	)
}
