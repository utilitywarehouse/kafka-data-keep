package internal

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConfig struct {
	Brokers       string
	BrokersDNSSrv string
}

func CompileRegexes(regexStr string) ([]*regexp.Regexp, error) {
	var regexes []*regexp.Regexp
	if regexStr != "" {
		for r := range strings.SplitSeq(regexStr, ",") {
			re, err := regexp.Compile(strings.TrimSpace(r))
			if err != nil {
				return nil, fmt.Errorf("invalid regex '%s': %w", r, err)
			}
			regexes = append(regexes, re)
		}
	}
	return regexes, nil
}

func SplitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func MatchesAny(s string, regexes []*regexp.Regexp) bool {
	for _, re := range regexes {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func KafkaConnOpts(cfg KafkaConfig) ([]kgo.Opt, error) {
	brokers, err := seedBrokers(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed constructing the kafka connection options: %w", err)
	}

	// todo[sb] add TLS options
	return []kgo.Opt{kgo.SeedBrokers(brokers...)}, nil
}

func seedBrokers(cfg KafkaConfig) ([]string, error) {
	if cfg.BrokersDNSSrv != "" {
		return resolveSeedBrokersFromDNS(cfg.BrokersDNSSrv)
	}
	if cfg.Brokers != "" {
		return SplitAndTrim(cfg.Brokers, ","), nil
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
