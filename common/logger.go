package common

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

func ConfigureLogger(debug bool, json bool) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	if !json {
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.StampMicro,
		})
	}

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
}
