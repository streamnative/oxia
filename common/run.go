package common

import (
	"github.com/rs/zerolog/log"
	"io"
)

func RunProcess(startProcess func() (io.Closer, error)) {
	ConfigureLogger()

	process, err := startProcess()
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the process")
	}

	profiler := RunProfiling()

	WaitUntilSignal(
		profiler,
		process,
	)
}
