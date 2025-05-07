// Copyright 2025 StreamNative, Inc.
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

package proofs

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/cmd/client/proofs/sequence"
	oxiacommon "github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/oxia"
)

var (
	Cmd = &cobra.Command{
		Use:   "proofs",
		Short: "Proof of correctness",
		Long:  `Proof of correctness for the oxia cluster.`,
	}
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", oxiacommon.DefaultPublicPort)
	Cmd.PersistentFlags().StringVarP(&common.Config.ServiceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	Cmd.PersistentFlags().StringVarP(&common.Config.Namespace, "namespace", "n", oxia.DefaultNamespace, "The Oxia namespace to use")
	Cmd.PersistentFlags().DurationVar(&common.Config.RequestTimeout, "request-timeout", oxia.DefaultRequestTimeout, "Requests timeout")

	Cmd.AddCommand(sequence.Cmd)
}
