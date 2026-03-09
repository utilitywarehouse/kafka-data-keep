package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

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

type OpsConfig struct {
	LogLevel    string
	LogFormat   string
	KGOLogLevel string
	MetricsPort string
}

func InitAppOps(ctx context.Context, cfg OpsConfig) error {
	// init log
	slog.SetDefault(NewSlogger(cfg.LogLevel, cfg.LogFormat))

	// init metrics
	if err := initMetricsServer(ctx, cfg.MetricsPort); err != nil {
		return err
	}
	return nil
}

func NewSlogger(level string, format string) *slog.Logger {
	l := ParseLogLevel(level)

	var handler slog.Handler
	switch format {
	case "json", "JSON":
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     l,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     l,
		})
	}
	return slog.New(handler)
}

func ParseLogLevel(level string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		return slog.LevelInfo
	}
	return l
}

var metricInit sync.Once

func initMetricsServer(ctx context.Context, port string) error {
	// using the prometheus exporter
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(prometheus.DefaultRegisterer),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return fmt.Errorf("failed initializing prometheus exporter: %w", err)
	}
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(exporter)))

	var metricInitErr error
	metricInit.Do(func() {
		// We use OpenTelemetry runtime/go metrics
		// go.opentelemetry.io/contrib/instrumentation/runtime
		//
		// As they can be expensive to collect, we disable the Prometheus
		// client also registering the same metrics
		prometheus.Unregister(collectors.NewGoCollector())

		metricInitErr = runtime.Start(runtime.WithMinimumReadMemStatsInterval(10 * time.Second))
	})

	if metricInitErr != nil {
		return fmt.Errorf("telemetry: failed to start runtime metric instrumentation: %w", metricInitErr)
	}

	// start the http server for metrics
	m := http.NewServeMux()
	m.Handle("/__/metrics", promhttp.Handler())

	// running the metrics on all interfaces, so it can be queried by Prometheus
	addr := fmt.Sprintf("0.0.0.0:%s", port)
	RunHTTPServer(ctx, addr, m, "metrics server")
	return nil
}

// RunHTTPServer starts an HTTP server on the specified address expected in the format host:port and will stop it when the provided context is done.
func RunHTTPServer(ctx context.Context, addr string, handler http.Handler, description string) {
	opServer := &http.Server{Addr: addr, ReadHeaderTimeout: 10 * time.Second, Handler: handler}
	errCh := make(chan error, 1)
	defer close(errCh)
	go func() {
		slog.InfoContext(ctx, "starting http server", "address", addr, "description", description)
		errCh <- opServer.ListenAndServe()
	}()

	//nolint:gosec // using the background context for clean-up, as the current context was cancelled
	go func() {
		select {
		case err := <-errCh:
			slog.InfoContext(ctx, "stopped http server", "error", err, "address", addr, "description", description)
		case <-ctx.Done():
			slog.InfoContext(ctx, "stopping http server", "address", addr, "description", description)
			//	give the server 10 seconds to shut down.
			sCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			if err := opServer.Shutdown(sCtx); err != nil {
				slog.ErrorContext(ctx, "failed to gracefully shutdown http server", "error", err, "address", addr, "description", description)
				// force close if the graceful shutdown failed.
				err := opServer.Close()
				if err != nil {
					slog.ErrorContext(ctx, "failed to forcefully shutdown http server", "error", err, "address", addr, "description", description)
				}
			}
		}
	}()
}
