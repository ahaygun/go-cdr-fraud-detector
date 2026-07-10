// Package platform holds the small cross-service plumbing every binary needs:
// structured logging, env config, and signal-based graceful shutdown.
// Intentionally tiny — domain logic lives in the individual services.
package platform

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// NewLogger returns a JSON structured logger tagged with the service name so
// logs from every container are uniform and greppable.
func NewLogger(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("service", service)
}

// SignalContext returns a context cancelled on SIGINT/SIGTERM. Every service
// blocks on ctx.Done() and shuts down gracefully — the stability baseline.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}

// Getenv returns the value of key, or fallback when it is unset.
func Getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
