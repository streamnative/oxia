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
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
)

func WaitUntilSignal(closers ...io.Closer) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	sig := <-c
	log.Info().
		Str("signal", sig.String()).
		Msg("Received signal, exiting")

	code := 0
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			log.Error().
				Err(err).
				Msg("Failed when shutting down server")
			os.Exit(1)
		}
	}

	if code == 0 {
		log.Info().Msg("Shutdown Completed")
	}
	os.Exit(code)
}
