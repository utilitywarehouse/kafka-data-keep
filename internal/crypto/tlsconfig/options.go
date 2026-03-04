package tlsconfig

type options struct {
	caPath, certPath, keyPath string
}

func defaultOpts() options {
	return options{
		caPath:   DefaultCAPath,
		certPath: DefaultCertPath,
		keyPath:  DefaultKeyPath,
	}
}

// Option is a function option
type Option func(*options)

// WithCAPath configures the path where the CA cert is read from
func WithCAPath(path string) Option {
	return func(o *options) {
		o.caPath = path
	}
}

// WithCertPath configures the path where the cert is read from
func WithCertPath(path string) Option {
	return func(o *options) {
		o.certPath = path
	}
}

// WithKeyPath configures the path where the private key is read from
func WithKeyPath(path string) Option {
	return func(o *options) {
		o.keyPath = path
	}
}
