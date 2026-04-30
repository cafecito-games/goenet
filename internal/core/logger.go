package core

import (
	"io"
	"log/slog"
)

func normalizeLogger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}

	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ComponentLogger returns a child logger tagged with the given component name.
func ComponentLogger(logger *slog.Logger, component string) *slog.Logger {
	return normalizeLogger(logger).With("component", component)
}
