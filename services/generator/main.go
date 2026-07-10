// Command generator produces synthetic CDR events onto cdr.raw.
//
// It emits a steady stream of "normal" traffic spread across a large MSISDN
// pool (so no single subscriber trips the velocity rule), and periodically
// injects a "dirty traffic" velocity burst: one MSISDN placing many calls in a
// couple of seconds — exactly what the fraud service should flag.
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/stream"
)

func main() {
	log := platform.NewLogger("generator")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	brokers := stream.Brokers(platform.Getenv("KAFKA_BROKERS", "localhost:29092"))
	rate := atoi(platform.Getenv("RATE", "10"))                 // normal calls per second
	poolSize := atoi(platform.Getenv("MSISDN_POOL", "200"))     // distinct normal callers
	burstEvery := seconds(platform.Getenv("BURST_EVERY", "15")) // inject a fraud burst this often
	burstSize := atoi(platform.Getenv("BURST_SIZE", "15"))      // calls per burst

	log.Info("starting", "kafka_brokers", brokers, "rate_per_sec", rate,
		"burst_every", burstEvery.String(), "burst_size", burstSize)

	if err := stream.EnsureTopics(ctx, brokers,
		stream.TopicSpec{Name: cdr.TopicRaw, Partitions: 3},
		stream.TopicSpec{Name: cdr.TopicAlert, Partitions: 3},
	); err != nil {
		log.Error("ensure topics failed", "err", err)
		return
	}

	w := stream.NewWriter(brokers, cdr.TopicRaw)
	defer w.Close()

	pool := makeMSISDNPool(poolSize)
	cells := makeCellPool(20)

	normalTick := time.NewTicker(time.Second / time.Duration(max(rate, 1)))
	defer normalTick.Stop()
	burstTick := time.NewTicker(burstEvery)
	defer burstTick.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return

		case <-normalTick.C:
			rec := randomCDR(pool[rand.IntN(len(pool))], pool, cells)
			if err := write(ctx, w, rec); err != nil && ctx.Err() == nil {
				log.Error("write failed", "err", err)
			}

		case <-burstTick.C:
			fraudster := pool[rand.IntN(len(pool))]
			cell := cells[rand.IntN(len(cells))]
			log.Warn("injecting velocity burst", "caller", fraudster, "calls", burstSize)
			for i := 0; i < burstSize; i++ {
				rec := randomCDR(fraudster, pool, []string{cell})
				if err := write(ctx, w, rec); err != nil && ctx.Err() == nil {
					log.Error("burst write failed", "err", err)
				}
			}
		}
	}
}

func write(ctx context.Context, w *kafka.Writer, rec cdr.CDR) error {
	val, err := rec.Marshal()
	if err != nil {
		return err
	}
	// Key = caller_msisdn → same subscriber always lands on the same partition.
	return w.WriteMessages(ctx, kafka.Message{Key: []byte(rec.CallerMSISDN), Value: val})
}

func randomCDR(caller string, pool, cells []string) cdr.CDR {
	return cdr.CDR{
		RecordID:     newID(),
		CallerMSISDN: caller,
		CalleeMSISDN: pool[rand.IntN(len(pool))],
		StartTime:    time.Now().UTC(),
		DurationSec:  rand.IntN(600),
		CellID:       cells[rand.IntN(len(cells))],
		CallType:     cdr.Voice,
		Termination:  "NORMAL",
	}
}

func makeMSISDNPool(n int) []string {
	pool := make([]string, max(n, 1))
	for i := range pool {
		pool[i] = fmt.Sprintf("+905%09d", rand.IntN(1_000_000_000))
	}
	return pool
}

func makeCellPool(n int) []string {
	cells := make([]string, max(n, 1))
	for i := range cells {
		cells[i] = fmt.Sprintf("CELL-%03d", i)
	}
	return cells
}

// newID returns a random 128-bit hex id. math/rand/v2 is fine here — these are
// synthetic records, not security-sensitive.
func newID() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], rand.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], rand.Uint64())
	return hex.EncodeToString(b[:])
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

func seconds(s string) time.Duration { return time.Duration(max(atoi(s), 1)) * time.Second }
