// Command fraud consumes cdr.raw, applies the velocity rule using a Redis
// sliding window, and emits alerts onto cdr.fraud.alert.
//
// Correctness baseline (Faz 1):
//   - partitioned by caller_msisdn (producer side) so a subscriber's calls are
//     handled by one consumer and the window stays consistent,
//   - MANUAL offset commit — the offset advances only after processing,
//   - idempotent processing — record_id dedup + record_id as the window member,
//     so redelivery never double-counts or double-alerts.
package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/rules"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/stream"
)

func main() {
	log := platform.NewLogger("fraud")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	brokers := stream.Brokers(platform.Getenv("KAFKA_BROKERS", "localhost:29092"))
	redisAddr := platform.Getenv("REDIS_ADDR", "localhost:6379")
	vel := rules.Velocity{
		WindowSeconds: atoi(platform.Getenv("VELOCITY_WINDOW_SEC", "60")),
		Threshold:     atoi(platform.Getenv("VELOCITY_THRESHOLD", "12")),
	}
	log.Info("starting", "kafka_brokers", brokers, "redis_addr", redisAddr,
		"velocity_window_sec", vel.WindowSeconds, "velocity_threshold", vel.Threshold)

	if err := stream.EnsureTopics(ctx, brokers,
		stream.TopicSpec{Name: cdr.TopicRaw, Partitions: 3},
		stream.TopicSpec{Name: cdr.TopicAlert, Partitions: 3},
	); err != nil {
		log.Error("ensure topics failed", "err", err)
		return
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	reader := stream.NewReader(brokers, "fraud", cdr.TopicRaw)
	defer reader.Close()
	writer := stream.NewWriter(brokers, cdr.TopicAlert)
	defer writer.Close()

	p := &processor{
		log:      log,
		rdb:      rdb,
		writer:   writer,
		vel:      vel,
		windowMs: int64(vel.WindowSeconds) * 1000,
		seenTTL:  time.Duration(vel.WindowSeconds*3) * time.Second,
	}

	log.Info("consuming", "topic", cdr.TopicRaw)
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("shutting down")
				return
			}
			log.Error("fetch failed", "err", err)
			continue
		}

		// Process with retry so we never skip a message on a transient error;
		// processing is idempotent, so retries are safe.
		for {
			if err := p.handle(ctx, m.Value); err == nil {
				break
			} else if ctx.Err() != nil {
				return
			} else {
				log.Error("process failed; retrying", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}

		if err := reader.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			log.Error("commit failed", "err", err)
		}
	}
}

type processor struct {
	log      *slog.Logger
	rdb      *redis.Client
	writer   *kafka.Writer
	vel      rules.Velocity
	windowMs int64
	seenTTL  time.Duration
}

func (p *processor) handle(ctx context.Context, value []byte) error {
	rec, err := cdr.UnmarshalCDR(value)
	if err != nil {
		// Poison message: skip it (a real DLQ comes in a later phase).
		p.log.Warn("skipping unparseable record", "err", err)
		return nil
	}

	already, err := seen(ctx, p.rdb, rec.RecordID)
	if err != nil {
		return err
	}
	if already {
		return nil // already handled — idempotent skip
	}

	count, err := slidingWindowCount(ctx, p.rdb, "velocity:"+rec.CallerMSISDN,
		rec.StartTime.UnixMilli(), p.windowMs, rec.RecordID)
	if err != nil {
		return err
	}

	if triggered, score, evidence := p.vel.Evaluate(count); triggered {
		alert := cdr.FraudAlert{
			RecordID:     rec.RecordID,
			CallerMSISDN: rec.CallerMSISDN,
			Rule:         cdr.RuleVelocity,
			Score:        score,
			Evidence:     evidence,
			DetectedAt:   time.Now().UTC(),
		}
		val, err := alert.Marshal()
		if err != nil {
			return err
		}
		if err := p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(rec.CallerMSISDN), Value: val}); err != nil {
			return err
		}
		p.log.Warn("FRAUD", "rule", cdr.RuleVelocity, "caller", rec.CallerMSISDN, "count", count, "score", score)
	}

	return markSeen(ctx, p.rdb, rec.RecordID, p.seenTTL)
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
