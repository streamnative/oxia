package client

import (
	"fmt"
	"oxia/cmd/client/delete"
	"oxia/cmd/client/get"
	"oxia/cmd/client/list"
	"oxia/cmd/client/put"
	"oxia/operator/resource"
	"oxia/oxia"
	"time"

	"github.com/spf13/cobra"
)

var (
	serviceAddr            string
	batchLingerMs          int64
	maxRequestsPerBatch    int
	batchRequestTimeoutSec int64
	batcherBufferSize      int

	Cmd = &cobra.Command{
		Use:   "client",
		Short: "Read/Write records",
		Long:  `Operations to get, create, delete, and modify key-value records in an oxia cluster`,
	}
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", resource.MetricsPort())
	Cmd.Flags().StringVarP(&serviceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	Cmd.Flags().Int64Var(&batchLingerMs, "batch-linger", int64(oxia.DefaultBatchLinger/time.Millisecond), "Batch linger in milliseconds")
	Cmd.Flags().IntVar(&maxRequestsPerBatch, "max-requests-per-batch", oxia.DefaultMaxRequestsPerBatch, "Maximum requests per batch")
	Cmd.Flags().Int64Var(&batchRequestTimeoutSec, "batch-request-timeout", int64(oxia.DefaultBatchRequestTimeout/time.Second), "Batch timeout in seconds")
	Cmd.Flags().IntVar(&batcherBufferSize, "batcher-buffer-size", oxia.DefaultBatcherBufferSize, "Batcher buffer size")

	Cmd.AddCommand(put.Cmd)
	Cmd.AddCommand(delete.Cmd)
	Cmd.AddCommand(get.Cmd)
	Cmd.AddCommand(list.Cmd)
}
