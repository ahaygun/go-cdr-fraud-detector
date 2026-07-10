// Package stream centralizes our Kafka conventions on top of segmentio/kafka-go
// so every service produces/consumes the same way:
//   - producers partition by message Key (we key by caller_msisdn),
//   - consumers use a group and commit offsets MANUALLY after processing.
package stream

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Brokers splits a comma-separated broker list (e.g. "kafka:9092").
func Brokers(csv string) []string { return strings.Split(csv, ",") }

// TopicSpec describes a topic to create.
type TopicSpec struct {
	Name       string
	Partitions int
}

// EnsureTopics creates the given topics if they don't exist. Idempotent and
// safe to call from every service on startup; retries while the broker warms up.
func EnsureTopics(ctx context.Context, brokers []string, specs ...TopicSpec) error {
	var lastErr error
	for attempt := 0; attempt < 15; attempt++ {
		if lastErr = createTopics(brokers, specs); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return lastErr
}

func createTopics(brokers []string, specs []TopicSpec) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()

	configs := make([]kafka.TopicConfig, len(specs))
	for i, s := range specs {
		configs[i] = kafka.TopicConfig{Topic: s.Name, NumPartitions: s.Partitions, ReplicationFactor: 1}
	}
	if err := cc.CreateTopics(configs...); err != nil {
		if errors.Is(err, kafka.TopicAlreadyExists) || strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

// NewWriter returns a producer that partitions by message Key (Hash balancer),
// so all events for one caller_msisdn land on the same partition — which keeps
// per-subscriber state consistent as consumers scale out.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
	}
}

// NewReader returns a consumer-group reader with NO auto-commit. Callers use
// FetchMessage + CommitMessages so an offset advances only after work is done.
func NewReader(brokers []string, group, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  group,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
}
