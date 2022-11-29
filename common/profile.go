package common

import (
	"context"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	_ "net/http/pprof"
	"runtime/pprof"
)

var (
	PprofEnable      bool
	PprofBindAddress string
)

func RunProfiling() io.Closer {
	s := &http.Server{
		Addr:    PprofBindAddress,
		Handler: http.DefaultServeMux,
	}

	if !PprofEnable {
		// Do not start pprof server
		return s
	}

	log.Info().Str("address", s.Addr).Msg("Starting pprof server")
	log.Info().Msgf("  use http://%s/debug/pprof to access the browser", s.Addr)
	log.Info().Msgf("  use `go tool pprof http://%s/debug/pprof/profile` to get pprof file(cpu info)", s.Addr)
	log.Info().Msgf("  use `go tool pprof http://%s/debug/pprof/heap` to get inuse_space file", s.Addr)
	log.Info().Msg("")

	go pprof.Do(context.Background(),
		pprof.Labels("oxia", "pprof"),
		func(_ context.Context) {
			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatal().
					Err(err).
					Str("component", "pprof").
					Msg("Unable to start debug profiling server")
			}
		})

	return s
}
