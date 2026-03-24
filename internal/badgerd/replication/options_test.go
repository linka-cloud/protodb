package replication

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionsTLSTable(t *testing.T) {
	t.Run("missing_server_key", func(t *testing.T) {
		o := &Options{serverCert: []byte("cert")}
		_, err := o.TLS()
		require.EqualError(t, err, "missing server key")
	})

	t.Run("missing_server_cert", func(t *testing.T) {
		o := &Options{serverKey: []byte("key")}
		_, err := o.TLS()
		require.EqualError(t, err, "missing server certificate")
	})

	t.Run("explicit_tls_config_passthrough", func(t *testing.T) {
		cfg := &tls.Config{MinVersion: tls.VersionTLS13}
		o := &Options{tlsConfig: cfg}

		got, err := o.TLS()
		require.NoError(t, err)
		assert.Same(t, cfg, got)
	})

	t.Run("invalid_x509_pair", func(t *testing.T) {
		o := &Options{serverCert: []byte("not pem"), serverKey: []byte("not pem")}
		_, err := o.TLS()
		require.Error(t, err)
	})

	t.Run("client_ca_sets_require_and_verify", func(t *testing.T) {
		certPEM, keyPEM := makeCert(t, "server")
		caPEM, _ := makeCert(t, "ca")
		o := &Options{serverCert: certPEM, serverKey: keyPEM, clientCA: caPEM}

		cfg, err := o.TLS()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
		require.NotNil(t, cfg.ClientCAs)
		assert.NotEmpty(t, cfg.Certificates)
	})
}

func makeCert(t *testing.T, cn string) ([]byte, []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}
