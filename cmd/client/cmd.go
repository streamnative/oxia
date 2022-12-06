package client

import (
	"fmt"
	"os"
	"oxia/cmd/client/common"
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
	batchLingerMs          int
	maxRequestsPerBatch    int
	batchRequestTimeoutSec int
	batcherBufferSize      int

	Cmd = &cobra.Command{
		Use:               "client",
		Short:             "Read/Write records",
		Long:              `Operations to get, create, delete, and modify key-value records in an oxia cluster`,
		PersistentPreRunE: start,
	}
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", resource.PublicPort())
	Cmd.PersistentFlags().StringVarP(&serviceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	Cmd.PersistentFlags().IntVar(&batchLingerMs, "batch-linger", int(oxia.DefaultBatchLinger/time.Millisecond), "Batch linger in milliseconds")
	Cmd.PersistentFlags().IntVar(&maxRequestsPerBatch, "max-requests-per-batch", oxia.DefaultMaxRequestsPerBatch, "Maximum requests per batch")
	Cmd.PersistentFlags().IntVar(&batchRequestTimeoutSec, "batch-request-timeout", int(oxia.DefaultBatchRequestTimeout/time.Second), "Batch timeout in seconds")
	Cmd.PersistentFlags().IntVar(&batcherBufferSize, "batcher-buffer-size", oxia.DefaultBatcherBufferSize, "Batcher buffer size")

	Cmd.AddCommand(put.Cmd)
	Cmd.AddCommand(delete.Cmd)
	Cmd.AddCommand(get.Cmd)
	Cmd.AddCommand(list.Cmd)
}

func start(cmd *cobra.Command, args []string) error {
	options, err := oxia.NewClientOptions(serviceAddr,
		oxia.WithBatchLinger(time.Duration(batchLingerMs)*time.Millisecond),
		oxia.WithBatchRequestTimeout(time.Duration(batchRequestTimeoutSec)*time.Second),
		oxia.WithMaxRequestsPerBatch(maxRequestsPerBatch),
	)
	if err != nil {
		return err
	}

	go common.QueryLoop(common.Queries, oxia.NewAsyncClient(options), os.Stdout)
	return nil
}
