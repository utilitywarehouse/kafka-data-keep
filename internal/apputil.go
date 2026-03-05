package internal

import (
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
)

type LogConfig struct {
	LogLevel    string
	LogFormat   string
	KGOLogLevel string
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

func InitGlobalLog(cfg LogConfig) {
	slog.SetDefault(NewSlogger(cfg.LogLevel, cfg.LogFormat))
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
