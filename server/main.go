package server

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"oxia/common"
	"syscall"
	"time"
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
	common.ConfigureLogger(common.LogDebug, common.LogJson)

	server, err := NewServer(&serverConfig{
		InternalServicePort: 8190,
		PublicServicePort:   9190,
	})
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the server")
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-c

		log.Info().
			Str("signal", sig.String()).
			Msg("Received signal, exiting")
		err := server.Close()
		if err != nil {
			log.Error().
				Err(err).
				Msg("Failed when shutting down server")
			os.Exit(1)
		} else {
			log.Info().Msg("Shutdown Completed")
			os.Exit(0)
		}
	}()

	for {
		time.Sleep(time.Hour)
	}
}
