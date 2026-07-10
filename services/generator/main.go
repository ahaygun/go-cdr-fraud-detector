// Command generator produces synthetic CDR events onto cdr.raw.
//
// Each subscriber has a "home" cell, so normal traffic stays in one place. It
// periodically injects two fraud scenarios:
//   - velocity burst      — one subscriber places many calls from home fast,
//   - impossible-travel    — one subscriber calls from home, then from a far
//     city seconds later (physically impossible).
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/geo"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/stream"
)

type subscriber struct {
	msisdn   string
	homeCell string
}

func main() {
	log := platform.NewLogger("generator")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	brokers := stream.Brokers(platform.Getenv("KAFKA_BROKERS", "localhost:29092"))
	rate := atoi(platform.Getenv("RATE", "10"))
	poolSize := atoi(platform.Getenv("MSISDN_POOL", "200"))
	burstEvery := seconds(platform.Getenv("BURST_EVERY", "15"))
	burstSize := atoi(platform.Getenv("BURST_SIZE", "15"))
	travelEvery := seconds(platform.Getenv("TRAVEL_EVERY", "20"))

	log.Info("starting", "kafka_brokers", brokers, "rate_per_sec", rate,
		"burst_every", burstEvery.String(), "travel_every", travelEvery.String())

	if err := stream.EnsureTopics(ctx, brokers,
		stream.TopicSpec{Name: cdr.TopicRaw, Partitions: 3},
		stream.TopicSpec{Name: cdr.TopicAlert, Partitions: 3},
	); err != nil {
		log.Error("ensure topics failed", "err", err)
		return
	}

	w := stream.NewWriter(brokers, cdr.TopicRaw)
	defer w.Close()

	pool := makePool(poolSize)

	normalTick := time.NewTicker(time.Second / time.Duration(max(rate, 1)))
	defer normalTick.Stop()
	burstTick := time.NewTicker(burstEvery)
	defer burstTick.Stop()
	travelTick := time.NewTicker(travelEvery)
	defer travelTick.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return

		case <-normalTick.C:
			s := pool[rand.IntN(len(pool))]
			emit(ctx, log, w, cdrFrom(s.msisdn, randomCallee(pool), s.homeCell))

		case <-burstTick.C:
			s := pool[rand.IntN(len(pool))]
			log.Warn("injecting velocity burst", "caller", s.msisdn, "calls", burstSize)
			for i := 0; i < burstSize; i++ {
				emit(ctx, log, w, cdrFrom(s.msisdn, randomCallee(pool), s.homeCell))
			}

		case <-travelTick.C:
			s := pool[rand.IntN(len(pool))]
			far := farCellFrom(s.homeCell)
			log.Warn("injecting impossible-travel", "caller", s.msisdn, "from", s.homeCell, "to", far)
			emit(ctx, log, w, cdrFrom(s.msisdn, randomCallee(pool), s.homeCell))
			emit(ctx, log, w, cdrFrom(s.msisdn, randomCallee(pool), far))
		}
	}
}

func emit(ctx context.Context, log *slog.Logger, w *kafka.Writer, rec cdr.CDR) {
	val, err := rec.Marshal()
	if err != nil {
		log.Error("marshal failed", "err", err)
		return
	}
	// Key = caller_msisdn → same subscriber always lands on the same partition.
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(rec.CallerMSISDN), Value: val}); err != nil && ctx.Err() == nil {
		log.Error("write failed", "err", err)
	}
}

func cdrFrom(caller, callee, cellID string) cdr.CDR {
	return cdr.CDR{
		RecordID:     newID(),
		CallerMSISDN: caller,
		CalleeMSISDN: callee,
		StartTime:    time.Now().UTC(),
		DurationSec:  rand.IntN(600),
		CellID:       cellID,
		CallType:     cdr.Voice,
		Termination:  "NORMAL",
	}
}

func makePool(n int) []subscriber {
	pool := make([]subscriber, max(n, 1))
	for i := range pool {
		pool[i] = subscriber{
			msisdn:   fmt.Sprintf("+905%09d", rand.IntN(1_000_000_000)),
			homeCell: geo.Catalog[rand.IntN(len(geo.Catalog))].ID,
		}
	}
	return pool
}

func randomCallee(pool []subscriber) string { return pool[rand.IntN(len(pool))].msisdn }

// farCellFrom returns a catalog cell far (>2000 km) from home — the destination
// for an injected impossible-travel jump.
func farCellFrom(homeID string) string {
	home, ok := geo.Lookup(homeID)
	if !ok {
		return homeID
	}
	var far []string
	for _, c := range geo.Catalog {
		if geo.HaversineKm(home.Lat, home.Lon, c.Lat, c.Lon) > 2000 {
			far = append(far, c.ID)
		}
	}
	if len(far) == 0 {
		return homeID
	}
	return far[rand.IntN(len(far))]
}

// newID returns a random 128-bit hex id. math/rand/v2 is fine — synthetic data.
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
