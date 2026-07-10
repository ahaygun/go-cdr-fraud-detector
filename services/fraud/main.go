// Command fraud consumes cdr.raw and applies the fraud rules, emitting alerts
// on cdr.fraud.alert.
//
//   - velocity          — too many calls per subscriber in a window (Redis ZSET)
//   - impossible-travel  — two calls too far apart to be physically possible;
//     needs the current cell's geo, fetched from subscriber-service over gRPC
//
// Correctness baseline: partitioned by caller_msisdn, MANUAL offset commit,
// idempotent processing (record_id dedup + record_id as the window member).
// The gRPC enrichment degrades gracefully: if subscriber-service is down,
// impossible-travel is skipped but velocity and the pipeline keep running.
package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cdrv1 "github.com/ahaygun/go-cdr-fraud-detector/gen/cdr/v1"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/geo"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/rules"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/stream"
)

const (
	grpcTimeout = 2 * time.Second
	lastLocTTL  = 24 * time.Hour
)

func main() {
	log := platform.NewLogger("fraud")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	brokers := stream.Brokers(platform.Getenv("KAFKA_BROKERS", "localhost:29092"))
	redisAddr := platform.Getenv("REDIS_ADDR", "localhost:6379")
	subscriberAddr := platform.Getenv("SUBSCRIBER_ADDR", "localhost:50051")
	vel := rules.Velocity{
		WindowSeconds: atoi(platform.Getenv("VELOCITY_WINDOW_SEC", "60")),
		Threshold:     atoi(platform.Getenv("VELOCITY_THRESHOLD", "12")),
	}
	travel := rules.ImpossibleTravel{
		MaxSpeedKmh: float64(atoi(platform.Getenv("IMPOSSIBLE_TRAVEL_MAX_KMH", "1000"))),
	}
	log.Info("starting", "kafka_brokers", brokers, "redis_addr", redisAddr,
		"subscriber_addr", subscriberAddr, "velocity_threshold", vel.Threshold,
		"impossible_travel_max_kmh", travel.MaxSpeedKmh)

	if err := stream.EnsureTopics(ctx, brokers,
		stream.TopicSpec{Name: cdr.TopicRaw, Partitions: 3},
		stream.TopicSpec{Name: cdr.TopicAlert, Partitions: 3},
	); err != nil {
		log.Error("ensure topics failed", "err", err)
		return
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	conn, err := grpc.NewClient(subscriberAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("grpc client failed", "err", err)
		return
	}
	defer conn.Close()
	ref := cdrv1.NewReferenceClient(conn)

	reader := stream.NewReader(brokers, "fraud", cdr.TopicRaw)
	defer reader.Close()
	writer := stream.NewWriter(brokers, cdr.TopicAlert)
	defer writer.Close()

	p := &processor{
		log:      log,
		rdb:      rdb,
		ref:      ref,
		writer:   writer,
		vel:      vel,
		travel:   travel,
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

		for { // retry until processed; processing is idempotent
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
	ref      cdrv1.ReferenceClient
	writer   *kafka.Writer
	vel      rules.Velocity
	travel   rules.ImpossibleTravel
	windowMs int64
	seenTTL  time.Duration
}

func (p *processor) handle(ctx context.Context, value []byte) error {
	rec, err := cdr.UnmarshalCDR(value)
	if err != nil {
		p.log.Warn("skipping unparseable record", "err", err)
		return nil
	}

	already, err := seen(ctx, p.rdb, rec.RecordID)
	if err != nil {
		return err
	}
	if already {
		return nil
	}

	// Rule 1 — velocity: self-contained, no external dependency.
	if err := p.checkVelocity(ctx, rec); err != nil {
		return err
	}

	// Rule 2 — impossible-travel: needs gRPC enrichment. Degrade gracefully so
	// a subscriber-service blip never blocks velocity or the pipeline.
	if err := p.checkImpossibleTravel(ctx, rec); err != nil {
		p.log.Warn("impossible-travel check skipped", "err", err, "caller", rec.CallerMSISDN)
	}

	return markSeen(ctx, p.rdb, rec.RecordID, p.seenTTL)
}

func (p *processor) checkVelocity(ctx context.Context, rec cdr.CDR) error {
	count, err := slidingWindowCount(ctx, p.rdb, "velocity:"+rec.CallerMSISDN,
		rec.StartTime.UnixMilli(), p.windowMs, rec.RecordID)
	if err != nil {
		return err
	}
	if triggered, score, evidence := p.vel.Evaluate(count); triggered {
		return p.emit(ctx, rec, cdr.RuleVelocity, score, evidence)
	}
	return nil
}

func (p *processor) checkImpossibleTravel(ctx context.Context, rec cdr.CDR) error {
	// Synchronous enrichment: fetch the current cell's coordinates over gRPC.
	cctx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()
	cell, err := p.ref.GetCell(cctx, &cdrv1.GetCellRequest{CellId: rec.CellID})
	if err != nil {
		return err
	}

	last, ok, err := getLastLocation(ctx, p.rdb, rec.CallerMSISDN)
	if err != nil {
		return err
	}
	current := lastLoc{Lat: cell.GetLat(), Lon: cell.GetLon(), At: rec.StartTime}

	var triggered bool
	var score float64
	var evidence string
	if ok {
		dist := geo.HaversineKm(last.Lat, last.Lon, current.Lat, current.Lon)
		dt := current.At.Sub(last.At)
		if dt < 0 {
			dt = -dt
		}
		triggered, score, evidence = p.travel.Evaluate(dist, dt)
	}

	// Advance the last-known location regardless of the emit outcome.
	if err := setLastLocation(ctx, p.rdb, rec.CallerMSISDN, current, lastLocTTL); err != nil {
		return err
	}

	if triggered {
		return p.emit(ctx, rec, cdr.RuleImpossibleTravel, score, evidence)
	}
	return nil
}

func (p *processor) emit(ctx context.Context, rec cdr.CDR, rule string, score float64, evidence string) error {
	alert := cdr.FraudAlert{
		RecordID:     rec.RecordID,
		CallerMSISDN: rec.CallerMSISDN,
		Rule:         rule,
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
	p.log.Warn("FRAUD", "rule", rule, "caller", rec.CallerMSISDN, "score", score, "evidence", evidence)
	return nil
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
