// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package core

import (
	"log/slog"
)

func normalizeLogger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}

	return slog.New(slog.DiscardHandler)
}

// ComponentLogger returns a child logger tagged with the given component name.
func ComponentLogger(logger *slog.Logger, component string) *slog.Logger {
	return normalizeLogger(logger).With("component", component)
}
