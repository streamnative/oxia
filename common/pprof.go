package common

import (
	"context"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	"runtime/pprof"
)

var (
	PprofEnable      bool
	PprofBindAddress string
)

// DoWithLabels attaches the labels to the current go-routine Pprof context,
// for the duration of the call to f
func DoWithLabels(labels map[string]string, f func()) {
	var l []string
	for k, v := range labels {
		l = append(l, k, v)
	}

	pprof.Do(
		context.Background(),
		pprof.Labels(l...),
		func(_ context.Context) {
			f()
		})
}

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

	DoWithLabels(map[string]string{
		"oxia": "pprof",
	}, func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().
				Err(err).
				Str("component", "pprof").
				Msg("Unable to start debug profiling server")
		}
	})

	return s
}
