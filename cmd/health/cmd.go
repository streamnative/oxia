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

package health

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/common/security"
)

type Config struct {
	Host    string
	Port    int
	Timeout time.Duration
	Service string
	// TLSEnabled turns on TLS when no other TLS option is set, e.g. to verify
	// the server against the system roots.
	TLSEnabled bool
	TLS        security.TLSOptions
}

func NewConfig() Config {
	return Config{
		Host:    "",
		Port:    constant.DefaultInternalPort,
		Timeout: 10 * time.Second,
		Service: "",
	}
}

// clientTLSConf turns on TLS when any TLS option is set: the client
// certificate is optional, so it can't be the only switch.
func (c *Config) clientTLSConf() (*tls.Config, error) {
	options := c.TLS
	enabled := c.TLSEnabled || options.CertFile != "" || options.KeyFile != "" ||
		options.TrustedCaFile != "" || options.ServerName != "" || options.InsecureSkipVerify ||
		options.MinVersion != 0 || options.MaxVersion != 0
	options.Enabled = &enabled
	return options.TryIntoClientTLSConf()
}

var (
	Cmd = &cobra.Command{
		Use:   "health",
		Short: "Oxia health probe",
		Long:  `Oxia tool to check the health endpoint of a running service`,
		Args:  cobra.NoArgs,
		RunE:  exec,
	}

	config = NewConfig()
)

func init() {
	Cmd.Flags().StringVar(&config.Host, "host", config.Host, "Server host")
	Cmd.Flags().IntVar(&config.Port, "port", config.Port, "Server port")
	Cmd.Flags().DurationVar(&config.Timeout, "timeout", config.Timeout, "Health check timeout")
	Cmd.Flags().StringVar(&config.Service, "service", config.Service, "Health check service")

	Cmd.Flags().BoolVar(&config.TLSEnabled, "tls", false, "Connect with tls (implied by any other --tls-* flag)")
	Cmd.Flags().StringVar(&config.TLS.CertFile, "tls-cert-file", "", "Tls client certificate file")
	Cmd.Flags().StringVar(&config.TLS.KeyFile, "tls-key-file", "", "Tls client key file")
	Cmd.Flags().Uint16Var(&config.TLS.MinVersion, "tls-min-version", 0, "Tls minimum version")
	Cmd.Flags().Uint16Var(&config.TLS.MaxVersion, "tls-max-version", 0, "Tls maximum version")
	Cmd.Flags().StringVar(&config.TLS.TrustedCaFile, "tls-trusted-ca-file", "", "Tls trusted ca file")
	Cmd.Flags().BoolVar(&config.TLS.InsecureSkipVerify, "tls-insecure-skip-verify", false, "Tls insecure skip verify")
	Cmd.Flags().StringVar(&config.TLS.ServerName, "tls-server-name", "", "Tls server name")

	Cmd.SilenceUsage = true
	Cmd.SilenceErrors = true
}

func exec(*cobra.Command, []string) error {
	tlsConf, err := config.clientTLSConf()
	if err != nil {
		return err
	}
	clientPool := rpc.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	serverAddress := fmt.Sprintf("%s:%d", config.Host, config.Port)

	client, err := clientPool.GetHealthRpc(serverAddress)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	req := &grpc_health_v1.HealthCheckRequest{Service: config.Service}

	resp, err := client.Check(ctx, req)
	if err != nil {
		return err
	}

	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return errors.New("unhealthy")
	}

	return nil
}
