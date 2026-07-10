// Command read-api is the visible surface of the pipeline. It consumes
// cdr.fraud.alert into Postgres (idempotently) and serves the flagged calls
// over HTTP so a human can see fraud being caught in real time.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/stream"
)

const schema = `
CREATE TABLE IF NOT EXISTS fraud_alerts (
	record_id     text NOT NULL,
	caller_msisdn text NOT NULL,
	rule          text NOT NULL,
	score         double precision NOT NULL,
	evidence      text NOT NULL,
	detected_at   timestamptz NOT NULL,
	PRIMARY KEY (record_id, rule)
);`

func main() {
	log := platform.NewLogger("read-api")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	brokers := stream.Brokers(platform.Getenv("KAFKA_BROKERS", "localhost:29092"))
	httpAddr := platform.Getenv("HTTP_ADDR", ":8080")
	dsn := platform.Getenv("POSTGRES_DSN", "postgres://cdr:cdr@localhost:5432/cdr?sslmode=disable")
	log.Info("starting", "http_addr", httpAddr, "kafka_brokers", brokers)

	go platform.ServeMetrics(ctx, platform.Getenv("METRICS_ADDR", ":9100"), log)

	pool, err := connectDB(ctx, dsn)
	if err != nil {
		log.Error("db connect failed", "err", err)
		return
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Error("schema init failed", "err", err)
		return
	}

	if err := stream.EnsureTopics(ctx, brokers,
		stream.TopicSpec{Name: cdr.TopicAlert, Partitions: 3},
	); err != nil {
		log.Error("ensure topics failed", "err", err)
		return
	}

	go consumeAlerts(ctx, log, brokers, pool)

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           routes(log, pool),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Info("http listening", "addr", httpAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server error", "err", err)
	}
	log.Info("shutting down")
}

func connectDB(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 15; i++ { // wait for Postgres to accept connections
		if err = pool.Ping(ctx); err == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, err
}

func consumeAlerts(ctx context.Context, log *slog.Logger, brokers []string, pool *pgxpool.Pool) {
	reader := stream.NewReader(brokers, "read-api", cdr.TopicAlert)
	defer reader.Close()

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("alert fetch failed", "err", err)
			continue
		}

		alert, err := cdr.UnmarshalAlert(m.Value)
		if err != nil {
			log.Warn("skipping unparseable alert", "err", err)
			_ = reader.CommitMessages(ctx, m)
			continue
		}

		for { // retry store until success; the upsert is idempotent
			if err := storeAlert(ctx, pool, alert); err == nil {
				break
			} else if ctx.Err() != nil {
				return
			} else {
				log.Error("store alert failed; retrying", "err", err)
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
		alertsStored.Inc()
		log.Info("alert stored", "caller", alert.CallerMSISDN, "rule", alert.Rule, "score", alert.Score)
	}
}

func storeAlert(ctx context.Context, pool *pgxpool.Pool, a cdr.FraudAlert) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO fraud_alerts (record_id, caller_msisdn, rule, score, evidence, detected_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (record_id, rule) DO NOTHING`,
		a.RecordID, a.CallerMSISDN, a.Rule, a.Score, a.Evidence, a.DetectedAt)
	return err
}

func routes(log *slog.Logger, pool *pgxpool.Pool) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"service":   "read-api",
			"endpoints": []string{"GET /alerts", "GET /healthz"},
		})
	})

	mux.HandleFunc("GET /alerts", func(w http.ResponseWriter, r *http.Request) {
		alerts, err := recentAlerts(r.Context(), pool, 100)
		if err != nil {
			log.Error("query alerts failed", "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"count": len(alerts), "alerts": alerts})
	})

	return mux
}

func recentAlerts(ctx context.Context, pool *pgxpool.Pool, limit int) ([]cdr.FraudAlert, error) {
	rows, err := pool.Query(ctx,
		`SELECT record_id, caller_msisdn, rule, score, evidence, detected_at
		 FROM fraud_alerts ORDER BY detected_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	alerts := make([]cdr.FraudAlert, 0, limit)
	for rows.Next() {
		var a cdr.FraudAlert
		if err := rows.Scan(&a.RecordID, &a.CallerMSISDN, &a.Rule, &a.Score, &a.Evidence, &a.DetectedAt); err != nil {
			return nil, err
		}
		alerts = append(alerts, a)
	}
	return alerts, rows.Err()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
