package internal

import (
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
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

func NewSlogger(level slog.Leveler) *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	})

	logger := slog.New(handler)
	return logger
}
