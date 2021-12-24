package server

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"oxia/common"
)

var (
	shards            uint32
	staticNodes       []string
	replicationFactor uint32

	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a storage node",
		Long:  `Long description`,
		Run:   main,
	}
)

func main(cmd *cobra.Command, args []string) {
	common.ConfigureLogger()

	server, err := NewServer(&serverConfig{
		InternalServicePort: 8190,
		PublicServicePort:   9190,
	})
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the server")
	}

	common.WaitUntilSignal(server)
}
