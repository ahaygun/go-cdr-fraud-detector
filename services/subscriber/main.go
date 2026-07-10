// Command subscriber is the gRPC reference-data service: subscriber profile /
// plan, destination tariff, and cell -> geo lookups used to enrich fraud checks.
// Faz 0: skeleton — boots, logs its wiring, idles until shutdown.
package main

import (
	"context"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
)

func main() {
	log := platform.NewLogger("subscriber")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	grpcAddr := platform.Getenv("GRPC_ADDR", ":50051")
	dsn := platform.Getenv("POSTGRES_DSN", "postgres://cdr:cdr@localhost:5432/cdr?sslmode=disable")
	log.Info("starting", "grpc_addr", grpcAddr, "postgres_dsn", dsn)

	// Faz 2: statik referans (plan / tarife / hücre→coğrafya) için gRPC sunucu.
	<-ctx.Done()
	log.Info("shutting down")
}
