// Command read-api serves the flagged calls (alert surface) and basic live
// stats over HTTP — the surface that makes the pipeline visible.
// Faz 0: skeleton — boots, logs its wiring, idles until shutdown.
package main

import (
	"context"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
)

func main() {
	log := platform.NewLogger("read-api")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	httpAddr := platform.Getenv("HTTP_ADDR", ":8080")
	dsn := platform.Getenv("POSTGRES_DSN", "postgres://cdr:cdr@localhost:5432/cdr?sslmode=disable")
	log.Info("starting", "http_addr", httpAddr, "postgres_dsn", dsn)

	// Faz 1: flag'lenen çağrıları listeleyen HTTP uç.
	<-ctx.Done()
	log.Info("shutting down")
}
