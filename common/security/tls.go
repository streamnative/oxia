package security

import (
	libtls "crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/pkg/errors"
	"os"
)

type TlsOption struct {
	// CertFile is the path to the server certificate file.
	CertFile string
	// KeyFile is the path to the private key file.
	KeyFile string
	// CipherSuites is a list of supported cipher suites.
	CipherSuites []uint16
	// MinVersion is the minimum TLS version supported.
	MinVersion uint16
	// MaxVersion is the maximum TLS version supported.
	MaxVersion uint16
	// TrustedCaFile is the path to the CA certificate.
	TrustedCaFile string
	// InsecureSkipVerify controls whether it verifies the certificate chain and host name.
	InsecureSkipVerify bool
	// ServerName is the expected server name (for SNI) used when connecting to the server.
	ServerName string
	// ClientAuth controls whether the server requires clients to authenticate with a certificate.
	ClientAuth bool
}

var (
	InvalidTlsCertFile = errors.New("Tls cert file path can not be empty.")
	InvalidTlsKeyFile  = errors.New("Tls key file path can not be empty.")
)

func (tls *TlsOption) IsConfigured() bool {
	return tls.CertFile != ""
}

func (tls *TlsOption) makeCommonConfig() (*libtls.Config, error) {
	if tls.CertFile == "" {
		return nil, InvalidTlsCertFile
	}
	if tls.KeyFile == "" {
		return nil, InvalidTlsKeyFile
	}

	// validate it first
	_, err := libtls.LoadX509KeyPair(tls.CertFile, tls.KeyFile)
	if err != nil {
		return nil, err
	}

	var minVersion uint16 = libtls.VersionTLS12
	if tls.MinVersion != 0 {
		minVersion = tls.MinVersion
	}

	tlsConf := libtls.Config{
		MinVersion:         minVersion,
		MaxVersion:         tls.MaxVersion,
		ServerName:         tls.ServerName,
		InsecureSkipVerify: tls.InsecureSkipVerify,
	}

	if len(tls.CipherSuites) > 0 {
		tlsConf.CipherSuites = tls.CipherSuites
	}
	return &tlsConf, nil
}

func (tls *TlsOption) trustedCertPool() (*x509.CertPool, error) {
	certPool := x509.NewCertPool()
	bPem, err := os.ReadFile(tls.TrustedCaFile)
	if err != nil {
		return nil, err
	}
	var block *pem.Block
	block, bPem = pem.Decode(bPem)
	if block != nil {
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certPool.AddCert(cert)
	}
	return certPool, nil
}

func (tls *TlsOption) MakeClientTlsConf() (*libtls.Config, error) {
	tlsConf, err := tls.makeCommonConfig()
	if err != nil {
		return nil, err
	}

	if len(tls.TrustedCaFile) > 0 {
		certPool, err := tls.trustedCertPool()
		if err != nil {
			return nil, err
		}
		tlsConf.RootCAs = certPool
	}

	tlsConf.GetClientCertificate = func(unused *libtls.CertificateRequestInfo) (cert *libtls.Certificate, err error) {
		c, err := libtls.LoadX509KeyPair(tls.CertFile, tls.KeyFile)
		return &c, err
	}
	return tlsConf, nil
}

func (tls *TlsOption) MakeServerTlsConf() (*libtls.Config, error) {
	tlsConf, err := tls.makeCommonConfig()
	if err != nil {
		return nil, err
	}
	tlsConf.GetCertificate = func(clientHello *libtls.ClientHelloInfo) (cert *libtls.Certificate, err error) {
		c, err := libtls.LoadX509KeyPair(tls.CertFile, tls.KeyFile)
		return &c, err
	}

	// auth type upgrading
	authType := libtls.NoClientCert
	if tls.TrustedCaFile != "" {
		authType = libtls.VerifyClientCertIfGiven
	}
	if tls.ClientAuth {
		authType = libtls.RequireAndVerifyClientCert
	}
	tlsConf.ClientAuth = authType

	if len(tls.TrustedCaFile) > 0 {
		certPool, err := tls.trustedCertPool()
		if err != nil {
			return nil, err
		}
		tlsConf.ClientCAs = certPool
	}

	return tlsConf, nil
}
