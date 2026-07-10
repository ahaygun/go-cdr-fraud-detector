// Package platform holds the small cross-service plumbing every binary needs:
// structured logging, env config, and signal-based graceful shutdown.
// Intentionally tiny — domain logic lives in the individual services.
package platform

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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

// ServeMetrics serves Prometheus metrics at /metrics and a liveness/readiness
// probe at /healthz on addr, until ctx is cancelled. Call it in a goroutine;
// metrics are read from the default registry (where promauto counters live).
func ServeMetrics(ctx context.Context, addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Info("metrics listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server error", "err", err)
	}
}
