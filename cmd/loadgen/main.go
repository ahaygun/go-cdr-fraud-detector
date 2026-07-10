// Command loadgen floods cdr.raw with synthetic CDR events to load-test the
// pipeline. It uses an async, batched producer and can run unbounded (RATE=0,
// to find max throughput) or at a fixed target rate (to measure steady-state
// latency). Throughput and p99 are read from Prometheus (fraud's
// cdr_processed_total rate and cdr_processing_latency_seconds).
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"math/rand/v2"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/geo"
)

func main() {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:29092"), ",")
	target := atoi(getenv("RATE", "0")) // msgs/s; 0 = unbounded (max throughput)
	dur := seconds(getenv("DURATION", "30"))
	workers := max(atoi(getenv("WORKERS", "8")), 1)

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cdr.TopicRaw,
		Balancer:     &kafka.Hash{},
		Async:        true, // fire-and-forget; the writer batches internally
		BatchSize:    500,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	var lim *rate.Limiter
	if target > 0 {
		lim = rate.NewLimiter(rate.Limit(target), max(target/10, 1))
	}

	log.Printf("loadgen: brokers=%v rate=%s duration=%s workers=%d",
		brokers, rateStr(target), dur, workers)

	var sent atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cells := geo.Catalog
			for ctx.Err() == nil {
				if lim != nil {
					if err := lim.Wait(ctx); err != nil {
						return
					}
				}
				rec := randomCDR(cells)
				val, err := rec.Marshal()
				if err != nil {
					continue
				}
				if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(rec.CallerMSISDN), Value: val}); err == nil {
					sent.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	_ = w.Close() // flush the async queue

	total := sent.Load()
	secs := dur.Seconds()
	fmt.Printf("\nSENT %d messages in %.0fs = %.0f msg/s (producer side)\n", total, secs, float64(total)/secs)
	fmt.Println("read fraud's cdr_processed_total rate + cdr_processing_latency_seconds (p99) from Prometheus")
}

func randomCDR(cells []geo.Cell) cdr.CDR {
	return cdr.CDR{
		RecordID:     newID(),
		CallerMSISDN: fmt.Sprintf("+905%09d", rand.IntN(1_000_000_000)),
		CalleeMSISDN: fmt.Sprintf("+905%09d", rand.IntN(1_000_000_000)),
		StartTime:    time.Now().UTC(),
		DurationSec:  rand.IntN(600),
		CellID:       cells[rand.IntN(len(cells))].ID,
		CallType:     cdr.Voice,
		Termination:  "NORMAL",
	}
}

func newID() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], rand.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], rand.Uint64())
	return hex.EncodeToString(b[:])
}

func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

func seconds(s string) time.Duration { return time.Duration(max(atoi(s), 1)) * time.Second }

func rateStr(target int) string {
	if target <= 0 {
		return "unbounded"
	}
	return strconv.Itoa(target) + "/s"
}
