// Command generator produces synthetic CDR events onto the cdr.raw Kafka topic.
// Faz 0: skeleton — boots, logs its wiring, idles until shutdown.
package main

import (
	"context"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
)

func main() {
	log := platform.NewLogger("generator")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	brokers := platform.Getenv("KAFKA_BROKERS", "localhost:29092")
	log.Info("starting", "kafka_brokers", brokers)

	// Faz 1: sentetik CDR üret → cdr.raw (+ "kirli trafik" modu).
	<-ctx.Done()
	log.Info("shutting down")
}
