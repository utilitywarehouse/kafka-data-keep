package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kslog"
	"github.com/utilitywarehouse/kafka-data-keep/internal"
	"github.com/utilitywarehouse/kafka-data-keep/internal/crypto/tlsconfig"
)

type Config struct {
	Brokers       string
	BrokersDNSSrv string
	MTLSAuth      bool
	MTLSCA        string
	MTLSCert      string
	MTLSKey       string
	LogLevel      string
	LogFormat     string
}

func connOpts(cfg Config) ([]kgo.Opt, error) {
	brokers, err := seedBrokers(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed constructing the kafka connection options: %w", err)
	}

	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}

	if cfg.MTLSAuth {
		tlsCfg, err := tlsconfig.New(
			tlsconfig.WithCAPath(cfg.MTLSCA),
			tlsconfig.WithCertPath(cfg.MTLSCert),
			tlsconfig.WithKeyPath(cfg.MTLSKey),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	return opts, nil
}

func BaseOpts(cfg Config) ([]kgo.Opt, error) {
	opts, err := connOpts(cfg)
	if err != nil {
		return nil, err
	}

	kgoLogger := internal.NewSlogger(cfg.LogLevel, cfg.LogFormat)
	opts = append(opts,
		kgo.WithHooks(kotel.NewMeter()), // record metrics
		kgo.WithLogger(kslog.New(kgoLogger)),
	)
	return opts, nil
}

func seedBrokers(cfg Config) ([]string, error) {
	if cfg.BrokersDNSSrv != "" {
		return resolveSeedBrokersFromDNS(cfg.BrokersDNSSrv)
	}
	if cfg.Brokers != "" {
		return internal.SplitAndTrim(cfg.Brokers, ","), nil
	}

	return nil, fmt.Errorf("no kafka seed brokers config was provided")
}

const (
	dnsSrvServiceName = "kafka"
	dnsSrvProto       = "tcp"
)

func resolveSeedBrokersFromDNS(srvAddress string) ([]string, error) {
	_, addrs, err := net.DefaultResolver.LookupSRV(context.Background(), dnsSrvServiceName, dnsSrvProto, srvAddress)
	if err != nil {
		return nil, fmt.Errorf("failed looking up SRV DNS entry at address :%s: %w", srvAddress, err)
	}

	brokers := make([]string, len(addrs))
	for i, addr := range addrs {
		brokers[i] = net.JoinHostPort(addr.Target, strconv.Itoa(int(addr.Port)))
	}
	return brokers, nil
}
