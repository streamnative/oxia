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
	"github.com/streamnative/oxia/common/security"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/flag"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/server"
	"github.com/streamnative/oxia/server/kv"
)

var (
	conf = server.Config{}

	peerTls   = security.TLSOption{}
	serverTls = security.TLSOption{}

	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	flag.PublicAddr(Cmd, &conf.PublicServiceAddr)
	flag.InternalAddr(Cmd, &conf.InternalServiceAddr)
	flag.MetricsAddr(Cmd, &conf.MetricsServiceAddr)
	Cmd.Flags().StringVar(&conf.DataDir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().StringVar(&conf.WalDir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
	Cmd.Flags().DurationVar(&conf.WalRetentionTime, "wal-retention-time", 1*time.Hour, "Retention time for the entries in the write-ahead-log")
	Cmd.Flags().BoolVar(&conf.WalSyncData, "wal-sync-data", true, "Whether to sync data in write-ahead-log")
	Cmd.Flags().Int64Var(&conf.DbBlockCacheMB, "db-cache-size-mb", kv.DefaultFactoryOptions.CacheSizeMB,
		"Max size of the shared DB cache")

	// server TLS section
	Cmd.Flags().StringVar(&serverTls.CertFile, "tls-cert-file", "", "Tls certificate file")
	Cmd.Flags().StringVar(&serverTls.KeyFile, "tls-key-file", "", "Tls key file")
	Cmd.Flags().Uint16Var(&serverTls.MinVersion, "tls-min-version", 0, "Tls minimum version")
	Cmd.Flags().Uint16Var(&serverTls.MaxVersion, "tls-max-version", 0, "Tls maximum version")
	Cmd.Flags().StringVar(&serverTls.TrustedCaFile, "tls-trusted-ca-file", "", "Tls trusted ca file")
	Cmd.Flags().BoolVar(&serverTls.InsecureSkipVerify, "tls-insecure-skip-verify", false, "Tls insecure skip verify")
	Cmd.Flags().BoolVar(&serverTls.ClientAuth, "tls-client-auth", false, "Tls client auth")

	// peer client TLS section
	Cmd.Flags().StringVar(&peerTls.CertFile, "peer-tls-cert-file", "", "Peer tls certificate file")
	Cmd.Flags().StringVar(&peerTls.KeyFile, "peer-tls-key-file", "", "Peer tls key file")
	Cmd.Flags().Uint16Var(&peerTls.MinVersion, "peer-tls-min-version", 0, "Peer tls minimum version")
	Cmd.Flags().Uint16Var(&peerTls.MaxVersion, "peer-tls-max-version", 0, "Peer tls maximum version")
	Cmd.Flags().StringVar(&peerTls.TrustedCaFile, "peer-tls-trusted-ca-file", "", "Peer tls trusted ca file")
	Cmd.Flags().BoolVar(&peerTls.InsecureSkipVerify, "peer-tls-insecure-skip-verify", false, "Peer tls insecure skip verify")
	Cmd.Flags().StringVar(&peerTls.ServerName, "peer-tls-server-name", "", "Peer tls server name")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		var err error
		if serverTls.IsConfigured() {
			if conf.ServerTLS, err = serverTls.MakeServerTLSConf(); err != nil {
				return nil, err
			}
		}
		if peerTls.IsConfigured() {
			if conf.PeerTLS, err = peerTls.MakeClientTLSConf(); err != nil {
				return nil, err
			}
		}
		return server.New(conf)
	})
}
