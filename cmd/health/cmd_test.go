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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

func TestHealthCmd(t *testing.T) {
	_health := health.NewServer()
	server, err := rpc.Default.StartGrpcServer("health", "localhost:0", func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, _health)
	}, nil, &auth.Options{}, nil)
	assert.NoError(t, err)
	defer func() {
		_ = server.Close()
	}()

	_health.SetServingStatus("serving", grpc_health_v1.HealthCheckResponse_SERVING)
	_health.SetServingStatus("not-serving", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	_health.SetServingStatus("unknown", grpc_health_v1.HealthCheckResponse_UNKNOWN)

	portArg := fmt.Sprintf("--port=%d", server.Port())

	for _, test := range []struct {
		name         string
		args         []string
		expectedCode codes.Code
	}{
		{"happy path", []string{portArg}, codes.OK},
		{"incorrect port", []string{"--port=1"}, codes.Unavailable},
		{"serving", []string{portArg, "--service=serving"}, codes.OK},
		{"not-serving", []string{portArg, "--service=not-serving"}, codes.Unknown},
		{"unknown", []string{portArg, "--service=unknown"}, codes.Unknown},
		{"invalid", []string{portArg, "--service=invalid"}, codes.NotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			config = NewConfig()

			Cmd.SetArgs(test.args)
			err := Cmd.Execute()

			assert.Equal(t, test.expectedCode, status.Code(err))
		})
	}
}

func TestHealthCmdTLS(t *testing.T) {
	dir := writeTestCerts(t)
	caArg := "--tls-trusted-ca-file=" + filepath.Join(dir, "ca.crt")
	certArg := "--tls-cert-file=" + filepath.Join(dir, "client.crt")
	keyArg := "--tls-key-file=" + filepath.Join(dir, "client.key")

	serverTLS := security.TLSOptions{
		CertFile: filepath.Join(dir, "server.crt"),
		KeyFile:  filepath.Join(dir, "server.key"),
	}
	tlsPortArg := fmt.Sprintf("--port=%d", startTLSHealthServer(t, serverTLS))
	serverTLS.TrustedCaFile = filepath.Join(dir, "ca.crt")
	serverTLS.ClientAuth = true
	mtlsPortArg := fmt.Sprintf("--port=%d", startTLSHealthServer(t, serverTLS))

	for _, test := range []struct {
		name         string
		args         []string
		expectedCode codes.Code
		expectedErr  string
	}{
		{"tls: plaintext", []string{tlsPortArg}, codes.Unavailable, ""},
		{"tls: trusted ca", []string{tlsPortArg, caArg}, codes.OK, ""},
		{"tls: system roots", []string{tlsPortArg, "--tls"}, codes.Unavailable, "failed to verify certificate"},
		{"tls: insecure skip verify", []string{tlsPortArg, "--tls-insecure-skip-verify"}, codes.OK, ""},
		{"tls: server name", []string{tlsPortArg, caArg, "--tls-server-name=localhost"}, codes.OK, ""},
		{"tls: server name not in certificate", []string{tlsPortArg, caArg, "--tls-server-name=other"},
			codes.Unavailable, "failed to verify certificate"},
		{"mtls: no client certificate", []string{mtlsPortArg, caArg}, codes.Unavailable, "certificate required"},
		{"mtls: client certificate", []string{mtlsPortArg, caArg, certArg, keyArg}, codes.OK, ""},
		{"mtls: certificate without key", []string{mtlsPortArg, caArg, certArg},
			codes.Unknown, security.ErrInvalidTLSKeyFile.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			config = NewConfig()

			// Dial the address the servers listen on: "localhost" may resolve
			// to ::1 first, and the refused ::1 dial would hide the TLS error.
			Cmd.SetArgs(append([]string{"--host=127.0.0.1"}, test.args...))
			err := Cmd.Execute()

			assert.Equal(t, test.expectedCode, status.Code(err))
			if test.expectedErr != "" {
				assert.ErrorContains(t, err, test.expectedErr)
			}
		})
	}
}

func startTLSHealthServer(t *testing.T, options security.TLSOptions) int {
	t.Helper()
	tlsConf, err := options.TryIntoServerTLSConf()
	require.NoError(t, err)

	server, err := rpc.Default.StartGrpcServer("health", "127.0.0.1:0", func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, health.NewServer())
	}, tlsConf, &auth.Options{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = server.Close()
	})
	return server.Port()
}

// writeTestCerts writes a CA (ca.crt) and the localhost/127.0.0.1
// certificates it issues to the server (server.crt, server.key) and the client
// (client.crt, client.key), and returns their directory.
func writeTestCerts(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	writePEM(t, filepath.Join(dir, "ca.crt"), "CERTIFICATE", caDER)

	for i, name := range []string{"server", "client"} {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)),
			Subject:      pkix.Name{CommonName: name},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		writePEM(t, filepath.Join(dir, name+".crt"), "CERTIFICATE", der)
		keyDER, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		writePEM(t, filepath.Join(dir, name+".key"), "EC PRIVATE KEY", keyDER)
	}
	return dir
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0600))
}
