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

package server

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	pb "google.golang.org/protobuf/proto"
	"oxia/common/batch"
	"oxia/proto"
	"oxia/server/wal"
	"sync"
	"time"
)

type writeResult struct {
	actualRequest *proto.WriteRequest
	offset        int64
	timestamp     uint64
	err           error
}

type writeTask struct {
	request       func(int64) *proto.WriteRequest
	actualRequest *proto.WriteRequest
	flush         bool
	result        chan *writeResult
}

func NewWriteTask(request func(int64) *proto.WriteRequest, flush bool) *writeTask {
	task := &writeTask{
		request: request,
		flush:   flush,
		result:  make(chan *writeResult),
	}
	return task
}

type walWriteBatch struct {
	locker           sync.Locker
	tasks            []*writeTask
	flush            bool
	wal              wal.Wal
	term             int64
	quorumAckTracker QuorumAckTracker
	log              zerolog.Logger
	ctx              context.Context
}

var _ batch.Batch = (*walWriteBatch)(nil)

func (l *walWriteBatch) Add(a any) {
	task := a.(*writeTask)
	l.tasks = append(l.tasks, task)
	l.flush = task.flush
}

func (l *walWriteBatch) Size() int {
	if l.flush || len(l.tasks) >= 10 {
		return 1
	} else {
		return 0
	}
}

func (l *walWriteBatch) Complete() {
	l.locker.Lock()
	defer l.locker.Unlock()

	newOffset := l.quorumAckTracker.NextOffset()
	timestamp := uint64(time.Now().UnixMilli())

	var requests []*proto.WriteRequest
	for _, task := range l.tasks {
		task.actualRequest = task.request(newOffset)

		requests = append(requests, task.actualRequest)

		l.log.Debug().
			Interface("req", task.actualRequest).
			Msg("Append operation")
	}

	value := &proto.LogEntryValue{
		Value: &proto.LogEntryValue_Requests{
			Requests: &proto.WriteRequests{
				Writes: requests,
			},
		}}
	marshalled, err := pb.Marshal(value)
	logEntry := &proto.LogEntry{
		Term:      l.term,
		Offset:    newOffset,
		Value:     marshalled,
		Timestamp: timestamp,
	}
	if err != nil {
		l.Fail(errors.Wrap(err, "oxia: failed to marshal log entry"))
	}

	if err = l.wal.AppendAsync(logEntry); err != nil {
		l.Fail(errors.Wrap(err, "oxia: failed to append to wal"))
	}

	go func() {

		// Sync the WAL outside the batcher, so that we can have multiple waiting
		// sync requests
		if err = l.wal.Sync(l.ctx); err != nil {
			l.Fail(errors.Wrap(err, "oxia: failed to sync the wal"))
		}

		l.quorumAckTracker.AdvanceHeadOffset(newOffset)

		for _, task := range l.tasks {
			task.result <- &writeResult{
				actualRequest: task.actualRequest,
				offset:        newOffset,
				err:           nil,
				timestamp:     timestamp,
			}
			close(task.result)
		}
	}()

}

func (l *walWriteBatch) Fail(err error) {
	for _, task := range l.tasks {
		task.result <- &writeResult{
			offset: wal.InvalidOffset,
			err:    err,
		}
		close(task.result)
	}
}

func NewWalWriteBatcher(locker sync.Locker, term int64, shardId uint32, wal wal.Wal, quorumAckTracker QuorumAckTracker, ctx context.Context) batch.Batcher {
	batcherFactory := batch.BatcherFactory{
		Linger:              2,
		MaxRequestsPerBatch: 1,
		BatcherBufferSize:   10,
	}
	return batcherFactory.NewBatcher(
		func() batch.Batch {
			return &walWriteBatch{
				locker:           locker,
				wal:              wal,
				term:             term,
				quorumAckTracker: quorumAckTracker,
				ctx:              ctx,
				log: log.With().
					Str("component", "wal-write-batcher").
					Uint32("shard", shardId).
					Logger(),
			}
		})
}
