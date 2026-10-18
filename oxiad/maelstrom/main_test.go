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

package main

import (
	"bufio"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/logging"
)

func TestLogLevelErrorError(t *testing.T) {
	require.EqualError(t, LogLevelError("verbose"), "unknown log level (verbose)")
}

func TestConfigureLogLevelAppliesLevel(t *testing.T) {
	restoreLogging(t)

	logLevelStr = "debug"
	require.NoError(t, configureLogLevel(nil, nil))
	require.Equal(t, slog.LevelDebug, logging.LogLevel)
}

func TestConfigureLogLevelRejectsUnknownLevel(t *testing.T) {
	restoreLogging(t)

	logLevelStr = "verbose"
	require.ErrorIs(t, configureLogLevel(nil, nil), LogLevelError("verbose"))
}

func TestHandleInitSkipsMessagesBeforeInit(t *testing.T) {
	previousNode, previousNodes := thisNode, allNodes
	t.Cleanup(func() { thisNode, allNodes = previousNode, previousNodes })
	lines := captureStdout(t)

	scanner := bufio.NewScanner(strings.NewReader(strings.Join([]string{
		`{"src":"c1","dest":"n2","body":{"type":"read","msg_id":1,"key":1}}`,
		`{"src":"c0","dest":"n2","body":{"type":"init","msg_id":2,"node_id":"n2","node_ids":["n1","n2","n3"]}}`,
	}, "\n")))
	handleInit(scanner)

	require.Equal(t, "n2", thisNode)
	require.Equal(t, []string{"n1", "n2", "n3"}, allNodes)
	msgType, msg, _ := parseRequest(receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgType("init_ok"), msgType)
	initOk := msg.(*Message[BaseMessageBody])
	require.Equal(t, "n2", initOk.Src)
	require.Equal(t, "c0", initOk.Dest)
	require.Equal(t, int64(2), *initOk.Body.InReplyTo)
}

func TestRunCoordinatorRejectsTooFewDataServers(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	err := runCoordinator(d, []*proto.DataServerIdentity{{Public: "n2", Internal: "n2"}})
	require.ErrorContains(t, err, "failed to seed coordinator namespaces")
}

// restoreLogging restores the logging configuration that the test changes.
func restoreLogging(t *testing.T) {
	t.Helper()
	previousLevelStr, previousLevel, previousLogger := logLevelStr, logging.LogLevel, slog.Default()
	t.Cleanup(func() {
		logLevelStr, logging.LogLevel = previousLevelStr, previousLevel
		slog.SetDefault(previousLogger)
	})
}
