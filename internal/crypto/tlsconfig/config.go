package tlsconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/johanbrandhorst/certify"
)

const (
	// DefaultRenewBefore configures by default how long before the client certificate
	// is expired before refreshing it from disk
	// It is set to 5 mins which is the minimum accepted value in cert-manager
	DefaultRenewBefore = 5 * time.Minute

	// DefaultCAPath is the name of the file containing the CA cert
	DefaultCAPath = "/certs/ca.crt"
	// DefaultCertPath is the name of the file containing the client cert
	DefaultCertPath = "/certs/tls.crt"
	// DefaultKeyPath is the name of the file containing the client private key
	DefaultKeyPath = "/certs/tls.key"
)

// New creates a tls.Config from a mounted kubernetes.io/tls secret
// CA cert, client cert and private key are read from disk and cached in memory
// Cache is refreshed from disk when the cert is about to expire
func New(opts ...Option) (*tls.Config, error) {
	o := defaultOpts()
	for _, f := range opts {
		f(&o)
	}

	for _, p := range []string{o.certPath, o.keyPath} {
		if _, err := os.Stat(p); err != nil {
			return nil, err
		}
	}

	var caCertPool *x509.CertPool
	if o.caPath != "" {
		caCert, err := os.ReadFile(o.caPath)
		if err != nil {
			return nil, err
		}
		caCertPool, err = x509.SystemCertPool()
		if err != nil {
			caCertPool = x509.NewCertPool()
		}
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate: %s", o.caPath)
		}
	}

	crt := &certify.Certify{
		Issuer: &issuer{
			certFile: o.certPath,
			keyFile:  o.keyPath,
		},
		Cache:       certify.NewMemCache(),
		RenewBefore: DefaultRenewBefore,
	}

	return &tls.Config{
		RootCAs:              caCertPool,
		ClientCAs:            caCertPool,
		MinVersion:           tls.VersionTLS12,
		GetCertificate:       crt.GetCertificate,
		GetClientCertificate: crt.GetClientCertificate,
	}, nil
}

type issuer struct {
	certFile, keyFile string
}

func (i *issuer) Issue(context.Context, string, *certify.CertConfig) (*tls.Certificate, error) {
	c, err := tls.LoadX509KeyPair(i.certFile, i.keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load key pair: %w", err)
	}
	// no need to check error since LoadX509KeyPair is parsing the cert as well
	// nolint:errcheck
	c.Leaf, _ = x509.ParseCertificate(c.Certificate[0])
	return &c, nil
}
