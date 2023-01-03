package client

import (
	"fmt"
	"github.com/spf13/cobra"
	"oxia/cmd/client/common"
	"oxia/cmd/client/delete"
	"oxia/cmd/client/get"
	"oxia/cmd/client/list"
	"oxia/cmd/client/put"
	"oxia/kubernetes"
	"oxia/oxia"
)

var (
	Cmd = &cobra.Command{
		Use:   "client",
		Short: "Read/Write records",
		Long:  `Operations to get, create, delete, and modify key-value records in an oxia cluster`,
	}
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", kubernetes.PublicPort.Port)
	Cmd.PersistentFlags().StringVarP(&common.Config.ServiceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	Cmd.PersistentFlags().DurationVar(&common.Config.BatchLinger, "batch-linger", oxia.DefaultBatchLinger, "Max time requests will be staged to be included in a batch")
	Cmd.PersistentFlags().IntVar(&common.Config.MaxRequestsPerBatch, "max-requests-per-batch", oxia.DefaultMaxRequestsPerBatch, "Maximum requests per batch")
	Cmd.PersistentFlags().DurationVar(&common.Config.RequestTimeout, "batch-request-timeout", oxia.DefaultRequestTimeout, "Batch timeout in seconds")
	Cmd.PersistentFlags().IntVar(&common.Config.BatcherBufferSize, "batcher-buffer-size", oxia.DefaultBatcherBufferSize, "Batcher buffer size")

	Cmd.AddCommand(put.Cmd)
	Cmd.AddCommand(delete.Cmd)
	Cmd.AddCommand(get.Cmd)
	Cmd.AddCommand(list.Cmd)
}
