// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"io"
	"net/http"
	_ "net/http/pprof"
	"runtime/pprof"

	"github.com/rs/zerolog/log"
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

	go DoWithLabels(map[string]string{
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
