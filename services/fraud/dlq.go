package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
)

// msgWriter is the part of *kafka.Writer the processor uses — an interface so
// the alert and dead-letter sinks can be faked in tests.
type msgWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// runWithRetry processes one record with bounded retries. On success it returns
// true (the offset should be committed). If every attempt fails, the record is
// routed to the dead-letter topic — so one poison message cannot block the
// partition forever — and it still returns true (commit and move on). Only if
// the dead-letter write ITSELF fails does it return false, leaving the offset
// uncommitted for redelivery rather than silently dropping the record.
func (p *processor) runWithRetry(ctx context.Context, value []byte, handle func(context.Context, []byte) error) bool {
	var err error
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		if err = handle(ctx, value); err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		p.log.Warn("process failed; retrying", "attempt", attempt, "max", p.maxAttempts, "err", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(p.retryBackoff):
		}
	}

	if dlqErr := p.deadLetter(ctx, value, err); dlqErr != nil {
		p.log.Error("dead-letter write failed; leaving uncommitted for redelivery", "err", dlqErr, "cause", err)
		return false
	}
	deadLettered.Inc()
	p.log.Error("record dead-lettered after exhausting retries", "attempts", p.maxAttempts, "err", err)
	return true
}

// deadLetter publishes the original record to cdr.dlq with the failure reason,
// so an operator can inspect what could not be processed and why.
func (p *processor) deadLetter(ctx context.Context, value []byte, cause error) error {
	return p.dlqWriter.WriteMessages(ctx, kafka.Message{
		Value: value,
		Headers: []kafka.Header{
			{Key: "error", Value: []byte(cause.Error())},
			{Key: "source", Value: []byte(cdr.TopicRaw)},
		},
	})
}
