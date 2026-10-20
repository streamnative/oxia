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

package metric

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	libtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func TestMustNewMeterProviderPanicsOnRegisterFailure(t *testing.T) {
	registerer := &fakeRegisterer{register: func(prometheus.Collector) error {
		return errors.New("already registered")
	}}

	assert.PanicsWithError(t, "failed to initialize Prometheus metrics exporter: cannot register the collector: "+
		"already registered", func() { mustNewMeterProvider(registerer) })
}

func TestPrometheusMetrics(t *testing.T) {
	metrics, err := Start("localhost:0", nil)
	assert.NoError(t, err)

	url := fmt.Sprintf("http://localhost:%d/metrics", metrics.Port())
	response, err := http.Get(url)
	assert.NoError(t, err)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}

	assert.Equal(t, 200, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	assert.NoError(t, err)

	// Looks like exposition format
	assert.Equal(t, "# HELP ", string(body[0:7]))

	err = metrics.Close()
	assert.NoError(t, err)

	response2, err := http.Get(url)
	assert.ErrorContains(t, err, "connection refused")
	assert.Nil(t, response2)

	if response2 != nil && response2.Body != nil {
		defer response2.Body.Close()
	}
}

func TestStartServesMetricsOverTLS(t *testing.T) {
	cert, roots := newSelfSignedCertificate(t)
	metrics, err := Start("127.0.0.1:0", &libtls.Config{
		Certificates: []libtls.Certificate{cert},
		MinVersion:   libtls.VersionTLS12,
	})
	require.NoError(t, err)
	defer metrics.Close()

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &libtls.Config{RootCAs: roots, MinVersion: libtls.VersionTLS12},
	}}
	defer client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fmt.Sprintf("https://127.0.0.1:%d/metrics", metrics.Port()), http.NoBody)
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()

	assert.Equal(t, http.StatusOK, response.StatusCode)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "# HELP ")
}

func TestStartRejectsTLSConfigWithoutCertificates(t *testing.T) {
	metrics, err := Start("127.0.0.1:0", &libtls.Config{MinVersion: libtls.VersionTLS12})

	assert.ErrorContains(t, err, "no certificates configured")
	assert.Nil(t, metrics)
}

func TestStartReturnsListenError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	metrics, err := Start(listener.Addr().String(), nil)

	assert.Error(t, err)
	assert.Nil(t, metrics)
}

func TestPrometheusMetricsServeLogsFailure(t *testing.T) {
	logs := captureLogs(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	p := &PrometheusMetrics{server: &http.Server{ReadHeaderTimeout: time.Second}}

	p.serve(listener)

	assert.Contains(t, logs.String(), "Failed to serve metrics")
}

// The OTel SDK defaults to 2000 attribute sets per instrument, collapsing the
// rest into an `otel.metric.overflow` series. Per-shard metrics can exceed that.
func TestNoCardinalityLimit(t *testing.T) {
	const name = "oxia_test_cardinality"
	const attributeSets = 2500

	counter, err := otel.Meter("oxia-test").Int64Counter(name)
	assert.NoError(t, err)

	for i := 0; i < attributeSets; i++ {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.Int("shard", i)))
	}

	families, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)

	series := 0
	for _, family := range families {
		if family.GetName() != name+"_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			series++
			for _, label := range m.GetLabel() {
				assert.NotEqual(t, "otel_metric_overflow", label.GetName())
			}
		}
	}
	assert.Equal(t, attributeSets, series)
}

// captureLogs routes the default logger to the returned buffer until the test ends.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	logs := &bytes.Buffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	return logs
}

type fakeRegisterer struct {
	prometheus.Registerer
	register func(prometheus.Collector) error
}

func (r *fakeRegisterer) Register(c prometheus.Collector) error {
	return r.register(c)
}

func newSelfSignedCertificate(t *testing.T) (libtls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return libtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, roots
}
