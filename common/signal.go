package common

import (
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func WaitUntilSignal(closer io.Closer) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
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
	}()

	for {
		time.Sleep(time.Hour)
	}
}
