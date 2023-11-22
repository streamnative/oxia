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
	"encoding/json"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"
)

const DefaultLogLevel = zerolog.InfoLevel

var (
	// LogLevel Used for flags
	LogLevel zerolog.Level
	// LogJson Used for flags
	LogJson bool
)

func ConfigureLogger() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	protoMarshal := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	zerolog.InterfaceMarshalFunc = func(i any) ([]byte, error) {
		if m, ok := i.(pb.Message); ok {
			return protoMarshal.Marshal(m)
		}
		return json.Marshal(i)
	}

	log.Logger = zerolog.New(os.Stdout).
		With().
		Timestamp().
		Stack().
		Logger()

	if !LogJson {
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.StampMicro,
		})
	}
	zerolog.SetGlobalLevel(LogLevel)
}
