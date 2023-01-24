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

package main

import (
	"bufio"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"os"
	"oxia/common"
	"oxia/coordinator/impl"
	"oxia/coordinator/model"
	"oxia/server"
	"path/filepath"
	"time"
)

var (
	logLevelStr string
	rootCmd     = &cobra.Command{
		Use:               "oxia-maelstrom",
		Short:             "Run oxia in Maelstrom mode",
		Long:              `Run oxia in Maelstrom mode`,
		PersistentPreRunE: configureLogLevel,
	}
)

type LogLevelError string

func (l LogLevelError) Error() string {
	return fmt.Sprintf("unknown log level (%s)", string(l))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&logLevelStr, "log-level", "l", common.DefaultLogLevel.String(), "Set logging level [disabled|trace|debug|info|warn|error|fatal|panic]")
	rootCmd.PersistentFlags().BoolVarP(&common.LogJson, "log-json", "j", false, "Print logs in JSON format")
}

func configureLogLevel(cmd *cobra.Command, args []string) error {
	logLevel, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		return LogLevelError(logLevelStr)
	}
	common.LogLevel = logLevel
	common.ConfigureLogger()
	return nil
}

var thisNode string
var allNodes []string

func handleInit(scanner *bufio.Scanner) {
	if !scanner.Scan() {
		log.Fatal().Msg("no init received")
	}

	line := scanner.Text()
	log.Info().RawJSON("line", []byte(line)).Msg("Got line")
	reqType, req, _ := parseRequest(line)
	if reqType != MsgTypeInit {
		log.Fatal().Msg("unexpected request")
	}

	init := req.(*Message[Init])

	thisNode = init.Body.NodeId
	allNodes = init.Body.NodesIds

	log.Info().
		Str("this-node", thisNode).
		Interface("all-nodes", allNodes).
		Msg("Received init request")

	sendResponse(Message[EmptyResponse]{
		Src:  thisNode,
		Dest: init.Src,
		Body: EmptyResponse{BaseMessageBody{
			Type:      "init_ok",
			MsgId:     msgIdGenerator.Add(1),
			InReplyTo: &init.Body.MsgId,
		}},
	})
}

func main() {
	common.ConfigureLogger()
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.StampMicro,
		NoColor:    true,
	})

	path, _ := os.Getwd()
	log.Info().Str("PWD", path).
		Msg("Starting Oxia in Maelstrom mode")

	scanner := bufio.NewScanner(os.Stdin)
	handleInit(scanner)

	// Start event loop to handle requests
	grpcProvider := newMaelstromGrpcProvider()
	replicationGrpcProvider := newMaelstromReplicationRpcProvider()
	dispatcher := newDispatcher(grpcProvider, replicationGrpcProvider)

	var servers []model.ServerAddress
	for _, node := range allNodes {
		if node != thisNode {
			servers = append(servers, model.ServerAddress{
				Public:   node,
				Internal: node,
			})
		}
	}

	dataDir, err := os.MkdirTemp("", "oxia-maelstrom")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create data dir")
	}

	if thisNode == "n1" {
		// First node is going to be the "coordinator"
		_, err := impl.NewCoordinator(
			impl.NewMetadataProviderFile(filepath.Join(dataDir, "cluster-status.json")),
			model.ClusterConfig{
				ReplicationFactor: 3,
				InitialShardCount: 1,
				Servers:           servers,
			}, newRpcProvider(dispatcher))
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create coordinator")
		}
	} else {
		// Any other node will be a storage node
		_, err := server.NewWithGrpcProvider(server.Config{
			MetricsServiceAddr: "",
			DataDir:            filepath.Join(dataDir, thisNode, "db"),
			WalDir:             filepath.Join(dataDir, thisNode, "wal"),
		}, grpcProvider, replicationGrpcProvider)
		if err != nil {
			return
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		rt, req, protoMsg := parseRequest(line)

		dispatcher.ReceivedMessage(rt, req, protoMsg)
	}
}
