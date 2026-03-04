package tlsconfig_test

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utilitywarehouse/uwos-go/crypto/tlsconfig"
)

func TestNew(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		c, err := tlsconfig.New(
			tlsconfig.WithCAPath("fixtures/valid/ca.crt"),
			tlsconfig.WithCertPath("fixtures/valid/tls.crt"),
			tlsconfig.WithKeyPath("fixtures/valid/tls.key"),
		)
		require.NoError(t, err)
		assert.NotNil(t, c.RootCAs)
		assert.NotNil(t, c.ClientCAs)

		crt, err := c.GetClientCertificate(&tls.CertificateRequestInfo{})
		require.NoError(t, err)
		assert.Equal(t, "test-client", crt.Leaf.Subject.CommonName)

		crt, err = c.GetCertificate(&tls.ClientHelloInfo{ServerName: "test-server"})
		require.NoError(t, err)
		assert.Equal(t, "test-client", crt.Leaf.Subject.CommonName)
	})

	t.Run("default to system ca pool cert", func(t *testing.T) {
		c, err := tlsconfig.New(
			tlsconfig.WithCAPath(""),
			tlsconfig.WithCertPath("fixtures/valid/tls.crt"),
			tlsconfig.WithKeyPath("fixtures/valid/tls.key"),
		)
		require.NoError(t, err)
		assert.Nil(t, c.RootCAs)
		assert.Nil(t, c.ClientCAs)
	})

	t.Run("invalid certs", func(t *testing.T) {
		c, err := tlsconfig.New(
			tlsconfig.WithCAPath(""),
			tlsconfig.WithCertPath("fixtures/invalid/tls.crt"),
			tlsconfig.WithKeyPath("fixtures/invalid/tls.key"),
		)
		require.NoError(t, err)

		_, err = c.GetClientCertificate(&tls.CertificateRequestInfo{})
		require.ErrorContains(t, err, "failed to load key pair")

		_, err = c.GetCertificate(&tls.ClientHelloInfo{ServerName: "test-server"})
		require.ErrorContains(t, err, "failed to load key pair")
	})

	t.Run("missing ca", func(t *testing.T) {
		_, err := tlsconfig.New(
			tlsconfig.WithCAPath("missing/ca.crt"),
			tlsconfig.WithCertPath("fixtures/valid/tls.crt"),
			tlsconfig.WithKeyPath("fixtures/valid/tls.key"),
		)
		assert.ErrorContains(t, err, "no such file or directory")
	})

	t.Run("invalid ca", func(t *testing.T) {
		_, err := tlsconfig.New(
			tlsconfig.WithCAPath("fixtures/invalid/ca.crt"),
			tlsconfig.WithCertPath("fixtures/valid/tls.crt"),
			tlsconfig.WithKeyPath("fixtures/valid/tls.key"),
		)
		assert.ErrorContains(t, err, "failed to parse CA certificate")
	})

	t.Run("missing cert", func(t *testing.T) {
		_, err := tlsconfig.New(
			tlsconfig.WithCAPath(""),
			tlsconfig.WithCertPath("missing/tls.crt"),
			tlsconfig.WithKeyPath("fixtures/valid/tls.key"),
		)
		assert.ErrorContains(t, err, "no such file or directory")
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := tlsconfig.New(
			tlsconfig.WithCAPath(""),
			tlsconfig.WithCertPath("fixtures/valid/tls.crt"),
			tlsconfig.WithKeyPath("missing/tls.key"),
		)
		assert.Error(t, err, "no such file or directory")
	})
}
