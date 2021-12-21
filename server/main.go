package main

import (
	"github.com/rs/zerolog/log"
	"oxia/common"
	"time"
)

func main() {
	common.ConfigureLogger(true, false)

	_, err := NewServer(&serverConfig{
		InternalServicePort: 8190,
		PublicServicePort:   9190,
	})
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the server")
	}

	for {
		time.Sleep(time.Second)
	}
}
