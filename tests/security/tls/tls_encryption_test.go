package tls

import (
	"fmt"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/security"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/server"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func getPeerTlsOption() (*security.TlsOption, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(pwd)
	caCertPath := filepath.Join(parentDir, "certs", "ca.crt")
	peerCertPath := filepath.Join(parentDir, "certs", "peer.crt")
	peerKeyPath := filepath.Join(parentDir, "certs", "peer.key")

	peerOption := security.TlsOption{
		CertFile:      peerCertPath,
		KeyFile:       peerKeyPath,
		TrustedCaFile: caCertPath,
	}
	return &peerOption, nil
}

func getClientTlsOption() (*security.TlsOption, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(pwd)
	caCertPath := filepath.Join(parentDir, "certs", "ca.crt")
	peerCertPath := filepath.Join(parentDir, "certs", "client.crt")
	peerKeyPath := filepath.Join(parentDir, "certs", "client.key")

	clientOption := security.TlsOption{
		CertFile:      peerCertPath,
		KeyFile:       peerKeyPath,
		TrustedCaFile: caCertPath,
	}
	return &clientOption, nil
}

func newTlsServer(t *testing.T) (s *server.Server, addr model.ServerAddress) {
	t.Helper()
	option, err := getPeerTlsOption()
	assert.NoError(t, err)
	serverTlsConf, err := option.MakeServerTlsConf()
	assert.NoError(t, err)
	peerTlsConf, err := option.MakeClientTlsConf()
	assert.NoError(t, err)

	s, err = server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
		PeerTls:                    peerTlsConf,
		ServerTls:                  serverTlsConf,
	})

	assert.NoError(t, err)

	addr = model.ServerAddress{
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func TestClusterHandshakeSuccess(t *testing.T) {
	s1, sa1 := newTlsServer(t)
	defer s1.Close()
	s2, sa2 := newTlsServer(t)
	defer s2.Close()
	s3, sa3 := newTlsServer(t)
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTlsOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTlsConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()
}

func TestClientHandshakeFailByNoTlsConfig(t *testing.T) {
	s1, sa1 := newTlsServer(t)
	defer s1.Close()
	s2, sa2 := newTlsServer(t)
	defer s2.Close()
	s3, sa3 := newTlsServer(t)
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTlsOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTlsConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	client, err := oxia.NewSyncClient(sa1.Public)
	assert.Nil(t, client)
}

func TestClientHandshakeByAuthFail(t *testing.T) {
	s1, sa1 := newTlsServer(t)
	defer s1.Close()
	s2, sa2 := newTlsServer(t)
	defer s2.Close()
	s3, sa3 := newTlsServer(t)
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTlsOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTlsConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	tlsOption, err := getClientTlsOption()
	// clear the CA file
	tlsOption.TrustedCaFile = ""
	assert.NoError(t, err)
	tlsConf, err = tlsOption.MakeClientTlsConf()
	assert.NoError(t, err)
	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithTls(tlsConf))
	assert.Nil(t, client)
}

func TestClientHandshakeWithInsecure(t *testing.T) {
	s1, sa1 := newTlsServer(t)
	defer s1.Close()
	s2, sa2 := newTlsServer(t)
	defer s2.Close()
	s3, sa3 := newTlsServer(t)
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTlsOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTlsConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	tlsOption, err := getClientTlsOption()
	// clear the CA file
	tlsOption.TrustedCaFile = ""
	tlsOption.InsecureSkipVerify = true
	assert.NoError(t, err)
	tlsConf, err = tlsOption.MakeClientTlsConf()
	assert.NoError(t, err)
	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithTls(tlsConf))
	assert.NoError(t, err)
	client.Close()
}

func TestClientHandshakeSuccess(t *testing.T) {
	s1, sa1 := newTlsServer(t)
	defer s1.Close()
	s2, sa2 := newTlsServer(t)
	defer s2.Close()
	s3, sa3 := newTlsServer(t)
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTlsOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTlsConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	tlsOption, err := getClientTlsOption()
	assert.NoError(t, err)
	tlsConf, err = tlsOption.MakeClientTlsConf()
	assert.NoError(t, err)
	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithTls(tlsConf))
	assert.NoError(t, err)
	client.Close()
}
