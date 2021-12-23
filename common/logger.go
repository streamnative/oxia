package common

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"os"
	"strings"
	"time"
)

func extractFileName(caller interface{}) string {
	s, ok := caller.(string)
	if !ok {
		return ""
	}

	parts := strings.Split(s, "/")
	return parts[len(parts)-1]
}

func ConfigureLogger(debug bool, json bool) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	log.Logger = zerolog.New(os.Stdout).
		With().
		Timestamp().
		//Caller().
		Logger()

	if !json {
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:          os.Stdout,
			TimeFormat:   time.StampMicro,
			FormatCaller: extractFileName,
		})
	}

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}
