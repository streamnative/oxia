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
	Cmd.Flags().Uint32VarP(&conf.NumShards, "shards", "s", 1, "Number of shards")
	Cmd.Flags().Uint32VarP(&conf.PublicServicePort, "port", "p", 8190, "Public service port")
	Cmd.Flags().StringVarP(&conf.AdvertisedPublicAddress, "advertised-address", "a", "", "Advertised address")
}

func main(cmd *cobra.Command, args []string) {
	common.ConfigureLogger()

	server, err := NewStandalone(&conf)
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the server")
	}

	common.WaitUntilSignal(server)
}
