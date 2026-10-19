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

package scan

import (
	"bytes"
	"log/slog"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

func TestScanExecSkipsControlRequests(t *testing.T) {
	dir := t.TempDir()
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  dir,
		Retention:   math.MaxInt64,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
	})
	w, err := factory.NewWal("default", 0, nil)
	require.NoError(t, err)
	// The leader writes a control request when it enables features
	appendValue(t, w, 0, &proto.LogEntryValue{Value: &proto.LogEntryValue_ControlRequest{
		ControlRequest: &proto.ControlRequest{Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{},
		}},
	}})
	appendValue(t, w, 1, &proto.LogEntryValue{Value: &proto.LogEntryValue_Requests{
		Requests: &proto.WriteRequests{Writes: []*proto.WriteRequest{{
			Puts: []*proto.PutRequest{{Key: "key", Value: []byte("value")}},
		}}},
	}})
	require.NoError(t, w.Close())

	common.WalOption.WalDir = dir
	common.WalOption.Namespace = "default"
	common.WalOption.Shard = 0
	logs := new(bytes.Buffer)
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	defer slog.SetDefault(previous)

	assert.NoError(t, exec(Cmd, nil))
	assert.Contains(t, logs.String(), "key=key")
}

func appendValue(t *testing.T, w wal.Wal, offset int64, value *proto.LogEntryValue) {
	t.Helper()
	data, err := value.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: offset, Value: data}))
}
