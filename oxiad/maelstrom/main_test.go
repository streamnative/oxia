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
	"os"
	"path/filepath"
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
	require.NoError(t, handleInit(scanner))

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
	_, err := runCoordinator(d, []*proto.DataServerIdentity{{Public: "n2", Internal: "n2"}})
	require.ErrorContains(t, err, "failed to seed coordinator namespaces")
}

func TestRunFailsWithoutInit(t *testing.T) {
	restoreNodes(t)

	err := run(newScanner(`{"src":"c1","dest":"n2","body":{"type":"read","msg_id":1,"key":1}}`), t.TempDir())

	require.ErrorIs(t, err, errNoInit)
}

func TestRunServesDataServerUntilInputEnds(t *testing.T) {
	restoreNodes(t)
	lines := captureStdout(t)

	err := run(newScanner(
		initMessage("n2"),
		`{"src":"c1","dest":"n2","body":{"type":"read","msg_id":5,"key":1}}`,
	), t.TempDir())

	require.NoError(t, err)
	received := receiveLines(t, lines, 2)
	msgType, _, _ := parseRequest(received[0])
	require.Equal(t, MsgType("init_ok"), msgType)
	msgType, msg, _ := parseRequest(received[1])
	require.Equal(t, MsgTypeError, msgType)
	require.Equal(t, int64(5), *msg.(*Message[ErrorResponse]).Body.InReplyTo)
}

func TestRunServesCoordinatorUntilInputEnds(t *testing.T) {
	restoreNodes(t)
	lines := captureStdout(t)

	require.NoError(t, run(newScanner(initMessage("n1")), t.TempDir()))

	msgType, _, _ := parseRequest(receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgType("init_ok"), msgType)
}

func TestRunFailsWhenCoordinatorCantStart(t *testing.T) {
	restoreNodes(t)
	captureStdout(t)

	err := run(newScanner(
		`{"src":"c0","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2"]}}`,
	), t.TempDir())

	require.ErrorContains(t, err, "failed to start coordinator")
}

func TestRunFailsWhenDataServerCantStart(t *testing.T) {
	restoreNodes(t)
	captureStdout(t)
	dataDir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(dataDir, nil, 0o600))

	err := run(newScanner(initMessage("n2")), dataDir)

	require.ErrorContains(t, err, "failed to create dataserver manifest")
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

// restoreNodes restores the node ids that the init message sets.
func restoreNodes(t *testing.T) {
	t.Helper()
	previousNode, previousNodes := thisNode, allNodes
	t.Cleanup(func() { thisNode, allNodes = previousNode, previousNodes })
}

func newScanner(lines ...string) *bufio.Scanner {
	return bufio.NewScanner(strings.NewReader(strings.Join(lines, "\n")))
}

func initMessage(node string) string {
	return `{"src":"c0","dest":"` + node + `","body":{"type":"init","msg_id":1,"node_id":"` + node +
		`","node_ids":["n1","n2","n3","n4"]}}`
}
