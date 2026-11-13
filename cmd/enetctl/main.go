// Command enetctl provides developer and diagnostic utilities for ENet and UDP
// transports built with goenet.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return execute(ctx, os.Args[1:], os.Stdout, os.Stderr)
}
