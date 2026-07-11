// Command fraud consumes cdr.raw and applies the fraud rules, emitting alerts
// on cdr.fraud.alert.
//
//   - velocity          — too many calls per subscriber in a window (Redis ZSET)
//   - impossible-travel  — two calls too far apart to be physically possible;
//     needs the current cell's geo, fetched from subscriber-service over gRPC
//
// Correctness baseline: partitioned by caller_msisdn, MANUAL offset commit,
// idempotent processing (record_id dedup + record_id as the window member).
// Alerts are de-duplicated per subscriber per window (one alert, not one per
// offending call). gRPC enrichment degrades gracefully: if subscriber-service
// is unavailable, impossible-travel is skipped but velocity and the pipeline
// keep running.
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
	irsf := rules.IRSF{
		WindowSeconds:  atoi(platform.Getenv("IRSF_WINDOW_SEC", "300")),
		SpendThreshold: float64(atoi(platform.Getenv("IRSF_SPEND_THRESHOLD", "50"))),
	}
	alertCooldown := time.Duration(atoi(platform.Getenv("ALERT_COOLDOWN_SEC", "60"))) * time.Second
	refCacheTTL := time.Duration(atoi(platform.Getenv("REFERENCE_CACHE_TTL_SEC", "300"))) * time.Second
	maxAttempts := atoi(platform.Getenv("MAX_ATTEMPTS", "5"))
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	log.Info("starting", "kafka_brokers", brokers, "redis_addr", redisAddr,
		"subscriber_addr", subscriberAddr, "velocity_threshold", vel.Threshold,
		"impossible_travel_max_kmh", travel.MaxSpeedKmh, "irsf_threshold", irsf.SpendThreshold,
		"alert_cooldown", alertCooldown.String(), "reference_cache_ttl", refCacheTTL.String(),
		"max_attempts", maxAttempts)

	go platform.ServeMetrics(ctx, platform.Getenv("METRICS_ADDR", ":9100"), log)

	if err := stream.EnsureTopics(ctx, brokers,
		stream.TopicSpec{Name: cdr.TopicRaw, Partitions: 3},
		stream.TopicSpec{Name: cdr.TopicAlert, Partitions: 3},
		stream.TopicSpec{Name: cdr.TopicDLQ, Partitions: 1},
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
	dlqWriter := stream.NewWriter(brokers, cdr.TopicDLQ)
	defer dlqWriter.Close()

	p := &processor{
		log:           log,
		rdb:           rdb,
		ref:           ref,
		writer:        writer,
		vel:           vel,
		travel:        travel,
		irsf:          irsf,
		windowMs:      int64(vel.WindowSeconds) * 1000,
		irsfWindowMs:  int64(irsf.WindowSeconds) * 1000,
		seenTTL:       time.Duration(vel.WindowSeconds*3) * time.Second,
		alertCooldown: alertCooldown,
		cellCache:     newRefCache[cellGeo](refCacheTTL),
		tariffCache:   newRefCache[tariffInfo](refCacheTTL),
		dlqWriter:     dlqWriter,
		maxAttempts:   maxAttempts,
		retryBackoff:  time.Second,
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

		// Bounded retries; a record that can never be processed is dead-lettered
		// so one poison message cannot block the partition forever.
		if !p.runWithRetry(ctx, m.Value, p.handle) {
			continue // left uncommitted → redelivered later (dead-letter write failed, or shutting down)
		}
		cdrProcessed.Inc()

		if err := reader.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			log.Error("commit failed", "err", err)
		}
	}
}

type processor struct {
	log           *slog.Logger
	rdb           *redis.Client
	ref           cdrv1.ReferenceClient
	writer        msgWriter
	vel           rules.Velocity
	travel        rules.ImpossibleTravel
	irsf          rules.IRSF
	windowMs      int64
	irsfWindowMs  int64
	seenTTL       time.Duration
	alertCooldown time.Duration
	cellCache     *refCache[cellGeo]
	tariffCache   *refCache[tariffInfo]
	dlqWriter     msgWriter
	maxAttempts   int
	retryBackoff  time.Duration
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

	if err := p.checkVelocity(ctx, rec); err != nil {
		return err
	}
	if err := p.checkImpossibleTravel(ctx, rec); err != nil {
		return err
	}
	if err := p.checkIRSF(ctx, rec); err != nil {
		return err
	}

	cdrLatency.Observe(time.Since(rec.StartTime).Seconds())
	return markSeen(ctx, p.rdb, rec.RecordID, p.seenTTL)
}

func (p *processor) checkVelocity(ctx context.Context, rec cdr.CDR) error {
	count, err := slidingWindowCount(ctx, p.rdb, "velocity:"+rec.CallerMSISDN,
		rec.StartTime.UnixMilli(), p.windowMs, rec.RecordID)
	if err != nil {
		return err
	}
	if triggered, score, evidence := p.vel.Evaluate(count); triggered {
		return p.maybeEmit(ctx, rec, cdr.RuleVelocity, score, evidence)
	}
	return nil
}

func (p *processor) checkImpossibleTravel(ctx context.Context, rec cdr.CDR) error {
	// Synchronous enrichment: the current cell's coordinates. Served from a
	// short-TTL cache-aside layer (the data is static) so steady-state lookups
	// stay off the wire; a miss falls back to gRPC. If it is unavailable and
	// uncached, skip this rule (graceful degradation).
	cg, err := p.getCellGeo(ctx, rec.CellID)
	if err != nil {
		p.log.Warn("impossible-travel skipped: enrichment unavailable", "err", err, "caller", rec.CallerMSISDN)
		return nil
	}

	current := lastLoc{Lat: cg.lat, Lon: cg.lon, At: rec.StartTime}
	last, ok, err := getLastLocation(ctx, p.rdb, rec.CallerMSISDN)
	if err != nil {
		return err
	}

	if ok {
		dist := geo.HaversineKm(last.Lat, last.Lon, current.Lat, current.Lon)
		dt := current.At.Sub(last.At)
		if dt < 0 {
			dt = -dt
		}
		if triggered, score, evidence := p.travel.Evaluate(dist, dt); triggered {
			// Emit BEFORE advancing the stored location, so a failed emit can be
			// retried against the same prior location instead of being lost.
			if err := p.maybeEmit(ctx, rec, cdr.RuleImpossibleTravel, score, evidence); err != nil {
				return err
			}
		}
	}

	return setLastLocation(ctx, p.rdb, rec.CallerMSISDN, current, lastLocTTL)
}

func (p *processor) checkIRSF(ctx context.Context, rec cdr.CDR) error {
	// Enrichment: is the destination premium, and at what rate? Cache-aside over
	// the static tariff catalog, same as the cell lookup above.
	ti, err := p.getTariff(ctx, rec.CalleeMSISDN)
	if err != nil {
		p.log.Warn("irsf skipped: enrichment unavailable", "err", err, "caller", rec.CallerMSISDN)
		return nil
	}
	if !ti.premium {
		return nil // only premium destinations accrue IRSF spend
	}

	cost := float64(rec.DurationSec) / 60.0 * ti.ratePerMin
	spend, err := premiumSpendInWindow(ctx, p.rdb, "spend:"+rec.CallerMSISDN,
		rec.StartTime.UnixMilli(), p.irsfWindowMs, rec.RecordID, cost)
	if err != nil {
		return err
	}

	if triggered, score, evidence := p.irsf.Evaluate(spend); triggered {
		return p.maybeEmit(ctx, rec, cdr.RuleIRSF, score, evidence)
	}
	return nil
}

// getCellGeo / getTariff wrap the gRPC reference lookups in the cache-aside
// layer: a fresh cache hit returns immediately; a miss makes the (timeout-bound)
// gRPC call and caches the result.
func (p *processor) getCellGeo(ctx context.Context, cellID string) (cellGeo, error) {
	return p.cellCache.get(cellID, time.Now(), func() (cellGeo, error) {
		cctx, cancel := context.WithTimeout(ctx, grpcTimeout)
		defer cancel()
		cell, err := p.ref.GetCell(cctx, &cdrv1.GetCellRequest{CellId: cellID})
		if err != nil {
			return cellGeo{}, err
		}
		return cellGeo{lat: cell.GetLat(), lon: cell.GetLon()}, nil
	})
}

func (p *processor) getTariff(ctx context.Context, dest string) (tariffInfo, error) {
	return p.tariffCache.get(dest, time.Now(), func() (tariffInfo, error) {
		cctx, cancel := context.WithTimeout(ctx, grpcTimeout)
		defer cancel()
		t, err := p.ref.GetTariff(cctx, &cdrv1.GetTariffRequest{Destination: dest})
		if err != nil {
			return tariffInfo{}, err
		}
		return tariffInfo{premium: t.GetPremium(), ratePerMin: t.GetRatePerMin()}, nil
	})
}

// maybeEmit emits a fraud alert, but at most once per (rule, subscriber) within
// the cooldown window — so a burst yields one alert, not one per offending call.
// The cooldown flag is set only AFTER a successful emit, so the emit stays safe
// to retry (the sink also de-duplicates on record_id + rule).
func (p *processor) maybeEmit(ctx context.Context, rec cdr.CDR, rule string, score float64, evidence string) error {
	key := "alerted:" + rule + ":" + rec.CallerMSISDN
	n, err := p.rdb.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil // already alerted this subscriber for this rule in the window
	}
	if err := p.emit(ctx, rec, rule, score, evidence); err != nil {
		return err
	}
	return p.rdb.Set(ctx, key, 1, p.alertCooldown).Err()
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
	fraudAlerts.WithLabelValues(rule).Inc()
	return nil
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
