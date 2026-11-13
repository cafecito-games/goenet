// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"

	"github.com/spf13/cobra"
)

type usageError struct {
	err error
}

func (u usageError) Error() string {
	return u.err.Error()
}

func (u usageError) Unwrap() error {
	return u.err
}

type cliState struct {
	stdout    io.Writer
	stderr    io.Writer
	logLevel  string
	logFormat string
	logger    *slog.Logger
}

func execute(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	command := newRootCommand(stdout, stderr)
	command.SetArgs(arguments)
	err := command.ExecuteContext(ctx)
	if err == nil {
		return 0
	}
	_, _ = fmt.Fprintf(stderr, "enetctl: %v\n", err)
	var badUsage usageError
	if errors.As(err, &badUsage) {
		return 2
	}
	return 1
}

func newRootCommand(stdout, stderr io.Writer) *cobra.Command {
	state := &cliState{
		stdout: stdout,
		stderr: stderr,
		logger: slog.New(slog.DiscardHandler),
	}

	root := &cobra.Command{
		Use:           "enetctl",
		Short:         "Diagnostic and testing CLI for goenet and UDP game transports",
		Version:       buildVersion(),
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			logger, err := buildLogger(state.stderr, state.logLevel, state.logFormat)
			if err != nil {
				return usageError{err: err}
			}
			state.logger = logger
			return nil
		},
	}

	root.SetOut(stdout)
	root.SetErr(stderr)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usageError{err: err}
	})

	root.PersistentFlags().StringVar(&state.logLevel, "log-level", "info", "log verbosity (debug, info, warn, error)")
	root.PersistentFlags().StringVar(&state.logFormat, "log-format", "auto", "log output format on stderr (auto, text, json)")

	root.AddCommand(newLagrelayCommand(state))
	return root
}

func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" {
		return "dev"
	}
	return info.Main.Version
}
