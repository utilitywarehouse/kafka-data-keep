package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
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
	EnablePProf bool
	PProfPort   string
}

// CloseFunc is a function that performs cleanup, similar to context.CancelFunc.
type CloseFunc func()

func InitAppOps(ctx context.Context, cfg OpsConfig) (CloseFunc, error) {
	// init log
	slog.SetDefault(NewSlogger(cfg.LogLevel, cfg.LogFormat))

	var closers []CloseFunc

	// init metrics
	closeMetrics, err := initMetricsServer(ctx, cfg.MetricsPort)
	if err != nil {
		return nil, err
	}
	closers = append(closers, closeMetrics)

	if cfg.EnablePProf {
		closers = append(closers, initPProfServer(ctx, cfg.PProfPort))
	}

	return func() {
		for _, c := range closers {
			c()
		}
	}, nil
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

const metricsPath = "/__/metrics"

func initMetricsServer(ctx context.Context, port string) (CloseFunc, error) {
	// using the prometheus exporter
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(prometheus.DefaultRegisterer),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed initializing prometheus exporter: %w", err)
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
		return nil, fmt.Errorf("telemetry: failed to start runtime metric instrumentation: %w", metricInitErr)
	}

	// start the http server for metrics
	m := http.NewServeMux()
	m.Handle(metricsPath, promhttp.Handler())

	// running the metrics on all interfaces, so it can be queried by Prometheus
	addr := fmt.Sprintf("0.0.0.0:%s", port)
	return RunHTTPServer(ctx, addr, m, "metrics server"), nil
}

func initPProfServer(ctx context.Context, port string) CloseFunc {
	m := http.NewServeMux()
	m.HandleFunc("/debug/pprof/", pprof.Index)
	m.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	m.HandleFunc("/debug/pprof/profile", pprof.Profile)
	m.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	m.HandleFunc("/debug/pprof/trace", pprof.Trace)
	m.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	m.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	m.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	m.Handle("/debug/pprof/block", pprof.Handler("block"))
	m.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	m.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))

	// running pprof only on localhost, so it cannot be abused remotely.
	addr := fmt.Sprintf("127.0.0.1:%s", port)
	return RunHTTPServer(ctx, addr, m, "pprof server")
}

// RunHTTPServer starts an HTTP server on the specified address expected in the format host:port.
// It returns a closer function that will gracefully shut down the server.
func RunHTTPServer(ctx context.Context, addr string, handler http.Handler, description string) CloseFunc {
	server := &http.Server{Addr: addr, ReadHeaderTimeout: 10 * time.Second, Handler: handler}
	go func() {
		slog.InfoContext(ctx, "starting http server", "address", addr, "description", description)
		if err := server.ListenAndServe(); err != nil {
			slog.ErrorContext(ctx, "error running the http server", "description", description, "error", err)
		}
		slog.InfoContext(ctx, "stopped http server", "description", description)
	}()

	return func() {
		slog.InfoContext(ctx, "stopping http server", "description", description)
		//	give the server 10 seconds to shut down.
		sCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := server.Shutdown(sCtx); err != nil {
			slog.ErrorContext(ctx, "failed to gracefully shutdown http server", "error", err, "description", description)
			// force close if the graceful shutdown failed.
			err := server.Close()
			if err != nil {
				slog.ErrorContext(ctx, "failed to forcefully shutdown http server", "error", err, "description", description)
			}
		}
	}
}
