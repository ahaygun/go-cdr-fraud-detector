// Command fraud consumes cdr.raw, applies the fraud rules (velocity /
// impossible-travel / IRSF) and emits alerts onto cdr.fraud.alert.
// Faz 0: skeleton — boots, logs its wiring, idles until shutdown.
package main

import (
	"context"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
)

func main() {
	log := platform.NewLogger("fraud")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	brokers := platform.Getenv("KAFKA_BROKERS", "localhost:29092")
	redisAddr := platform.Getenv("REDIS_ADDR", "localhost:6379")
	log.Info("starting", "kafka_brokers", brokers, "redis_addr", redisAddr)

	// Faz 1: cdr.raw tüket → velocity kuralı (Redis pencere) → cdr.fraud.alert.
	<-ctx.Done()
	log.Info("shutting down")
}
