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

package client

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/cmd/client/del"
	"github.com/streamnative/oxia/cmd/client/get"
	"github.com/streamnative/oxia/cmd/client/list"
	"github.com/streamnative/oxia/cmd/client/notifications"
	"github.com/streamnative/oxia/cmd/client/put"
	oxia_common "github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/oxia"
)

var (
	Cmd = &cobra.Command{
		Use:   "client",
		Short: "Read/Write records",
		Long:  `Operations to get, create, delete, and modify key-value records in an oxia cluster`,
	}
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", oxia_common.DefaultPublicPort)
	Cmd.PersistentFlags().StringVarP(&common.Config.ServiceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	Cmd.PersistentFlags().StringVarP(&common.Config.Namespace, "namespace", "n", oxia.DefaultNamespace, "The Oxia namespace to use")
	Cmd.PersistentFlags().DurationVar(&common.Config.BatchLinger, "batch-linger", oxia.DefaultBatchLinger, "Max time requests will be staged to be included in a batch")
	Cmd.PersistentFlags().IntVar(&common.Config.MaxRequestsPerBatch, "max-requests-per-batch", oxia.DefaultMaxRequestsPerBatch, "Maximum requests per batch")
	Cmd.PersistentFlags().DurationVar(&common.Config.RequestTimeout, "request-timeout", oxia.DefaultRequestTimeout, "Requests timeout")

	Cmd.AddCommand(put.Cmd)
	Cmd.AddCommand(del.Cmd)
	Cmd.AddCommand(get.Cmd)
	Cmd.AddCommand(list.Cmd)
	Cmd.AddCommand(notifications.Cmd)
}
