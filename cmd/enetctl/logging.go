// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lmittmann/tint"
)

func buildLogger(stderr io.Writer, levelName, formatName string) (*slog.Logger, error) {
	level, err := parseLogLevel(levelName)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(strings.TrimSpace(formatName)) {
	case "", "auto":
		return slog.New(tint.NewTextHandler(stderr, &tint.Options{
			Level:      level,
			TimeFormat: time.TimeOnly,
			NoColor:    !shouldColorize(stderr),
		})), nil
	case "text":
		return slog.New(tint.NewTextHandler(stderr, &tint.Options{
			Level:      level,
			TimeFormat: time.TimeOnly,
			NoColor:    true,
		})), nil
	case "json":
		return slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{
			Level: level,
		})), nil
	default:
		return nil, fmt.Errorf("unknown --log-format %q (expected auto, text, or json)", formatName)
	}
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown --log-level %q (expected debug, info, warn, or error)", value)
	}
}

func shouldColorize(writer io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	file, ok := writer.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
