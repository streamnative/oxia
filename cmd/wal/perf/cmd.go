// Copyright 2023-2026 The Oxia Authors
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

package perf

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/bmizerany/perks/quantile"
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/common/proto"
)

type perfOptions struct {
	entryCount      int64
	entrySize       int64
	readerCount     int64
	segmentSize     int64
	syncData        bool
	retentionSecond int64
}

var (
	options = perfOptions{}
	Cmd     = &cobra.Command{
		Use:   "perf",
		Short: "Performance test",
		Args:  cobra.NoArgs,
		RunE:  run,
	}
)

func init() {
	Cmd.Flags().Int64Var(&options.entryCount, "entry-count", 1000000, "number of entries to write")
	Cmd.Flags().Int64Var(&options.entrySize, "entry-size", 100, "size of each entry")
	Cmd.Flags().Int64Var(&options.readerCount, "reader-count", 1, "number of readers")
	Cmd.Flags().Int64Var(&options.segmentSize, "segment-size", 1024*1024, "size of each segment")
	Cmd.Flags().BoolVar(&options.syncData, "sync-data", true, "whether to sync data after each write")
	Cmd.Flags().Int64Var(&options.retentionSecond, "retention-second", 3600, "retention time for the wal")
}

func run(cmd *cobra.Command, _ []string) error {
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: int32(options.segmentSize),
		SyncData:    options.syncData,
	})
	writeAheadLog, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	if err != nil {
		panic(err)
	}
	defer writeAheadLog.Close()

	threadTimesArray := make([]int64, options.readerCount+1)
	writeEntryTimesArray := make([]int64, options.entryCount)
	readEntryTimesArray := make([]int64, (options.readerCount)*options.entryCount)
	data := make([]byte, options.entrySize)
	for i := 0; i < len(data); i++ {
		data[i] = 'a'
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		runWriter(writeAheadLog, data, threadTimesArray, writeEntryTimesArray)
	})

	for i := int64(0); i < options.readerCount; i++ {
		readerIdx := i
		wg.Go(func() {
			runReader(writeAheadLog, readerIdx, threadTimesArray, readEntryTimesArray)
		})
	}

	wg.Wait()
	return printResults(cmd.OutOrStdout(), threadTimesArray, writeEntryTimesArray, readEntryTimesArray)
}

func runWriter(writeAheadLog wal.Wal, data []byte, threadTimesArray, writeEntryTimesArray []int64) {
	n := time.Now().UnixMicro()

	writeGroup := sync.WaitGroup{}
	for i := int64(0); i < options.entryCount; i++ {
		writeGroup.Add(1)
		entry := &proto.LogEntry{
			Term:      1,
			Offset:    i,
			Value:     data,
			Timestamp: uint64(time.Now().UnixMicro()),
		}
		writeAheadLog.AppendAndSync(entry, func(_ uint32, err error) {
			writeGroup.Done()
			writeEntryTimesArray[i] = time.Now().UnixMicro() - int64(entry.Timestamp)
			if err != nil {
				panic(err)
			}
		})
	}
	writeGroup.Wait()
	threadTimesArray[0] = time.Now().UnixMicro() - n
	slog.Info("writer thread has done", slog.Int64("Elapse", threadTimesArray[0]))
}

func runReader(writeAheadLog wal.Wal, readerIdx int64, threadTimesArray, readEntryTimesArray []int64) {
	n := time.Now().UnixMicro()

	reader, err := writeAheadLog.NewReader(wal.InvalidOffset)
	if err != nil {
		panic(err)
	}

	for i := int64(0); i < options.entryCount; i++ {
		for {
			if !reader.HasNext() {
				continue
			}

			entry, _, _, err := reader.ReadNext()
			if err != nil {
				panic(err)
			}
			readEntryTimesArray[readerIdx*options.entryCount+i] = time.Now().UnixMicro() - int64(entry.Timestamp)
			break
		}
	}

	threadTimesArray[readerIdx+1] = time.Now().UnixMicro() - n
	slog.Info("reader thread has done",
		slog.Int64("Elapse", threadTimesArray[readerIdx+1]),
		slog.Int64("ReaderIdx", readerIdx),
	)
}

func printResults(out io.Writer, threadTimesArray, writeEntryTimesArray, readEntryTimesArray []int64) error {
	for i := 0; i < len(threadTimesArray); i++ {
		threadName := "Writer"
		if i > 0 {
			threadName = fmt.Sprintf("Reader%d", i-1)
		}

		if _, err := fmt.Fprintf(out, "Thread: %s, Total entry: %d, Total time: %s, Throughput: %f ops/s\n",
			threadName,
			options.entryCount,
			time.Duration(threadTimesArray[i])*time.Microsecond,
			float64(options.entryCount)/float64(threadTimesArray[i])*1000000); err != nil {
			return err
		}
	}

	if err := printLatench(out, "Writer", writeEntryTimesArray); err != nil {
		return err
	}
	return printLatench(out, "Reader", readEntryTimesArray)
}

const (
	p50  = 0.5
	p90  = 0.9
	p99  = 0.99
	p999 = 0.999
)

func printLatench(out io.Writer, name string, datas []int64) error {
	latencies := quantile.NewTargeted(p50, p90, p99, p999)
	for _, t := range datas {
		latencies.Insert(float64(t))
	}
	_, err := fmt.Fprintf(out, `
Histogram of %s latency:
P50:  %s
P90:  %s 
P99:  %s 
P999: %s
`, name,
		time.Duration(latencies.Query(p50))*time.Microsecond,
		time.Duration(latencies.Query(p90))*time.Microsecond,
		time.Duration(latencies.Query(p99))*time.Microsecond,
		time.Duration(latencies.Query(p999))*time.Microsecond,
	)
	return err
}
