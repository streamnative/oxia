package common

import (
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
)

func WaitUntilSignal(closer io.Closer) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for {
		sig := <-c
		log.Info().
			Str("signal", sig.String()).
			Msg("Received signal, exiting")
		err := closer.Close()
		if err != nil {
			log.Error().
				Err(err).
				Msg("Failed when shutting down server")
			os.Exit(1)
		} else {
			log.Info().Msg("Shutdown Completed")
			os.Exit(0)
		}
	}
}
