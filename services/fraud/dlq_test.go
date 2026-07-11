package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"
)

// fakeWriter records the messages written to it, and can be made to fail.
type fakeWriter struct {
	msgs []kafka.Message
	err  error
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func newTestProcessor(dlq msgWriter) *processor {
	return &processor{
		log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		dlqWriter:    dlq,
		maxAttempts:  3,
		retryBackoff: 0,
	}
}

func TestRunWithRetry_CommitsOnSuccess(t *testing.T) {
	dlq := &fakeWriter{}
	p := newTestProcessor(dlq)
	calls := 0

	commit := p.runWithRetry(context.Background(), []byte("ok"), func(context.Context, []byte) error {
		calls++
		return nil
	})

	if !commit {
		t.Fatal("want commit=true on success")
	}
	if calls != 1 {
		t.Fatalf("want 1 handle call, got %d", calls)
	}
	if len(dlq.msgs) != 0 {
		t.Fatalf("want no dead-letter on success, got %d", len(dlq.msgs))
	}
}

func TestRunWithRetry_DeadLettersAfterMaxAttempts(t *testing.T) {
	dlq := &fakeWriter{}
	p := newTestProcessor(dlq)
	calls := 0
	boom := errors.New("boom")

	commit := p.runWithRetry(context.Background(), []byte("poison"), func(context.Context, []byte) error {
		calls++
		return boom
	})

	if !commit {
		t.Fatal("want commit=true after dead-lettering (move past the poison record)")
	}
	if calls != 3 {
		t.Fatalf("want handle tried maxAttempts=3 times, got %d", calls)
	}
	if len(dlq.msgs) != 1 {
		t.Fatalf("want exactly 1 dead-letter, got %d", len(dlq.msgs))
	}
	if string(dlq.msgs[0].Value) != "poison" {
		t.Fatalf("dead-letter should carry the original record, got %q", dlq.msgs[0].Value)
	}
	var hasErr bool
	for _, h := range dlq.msgs[0].Headers {
		if h.Key == "error" && string(h.Value) == "boom" {
			hasErr = true
		}
	}
	if !hasErr {
		t.Fatal("dead-letter should carry the failure reason in an 'error' header")
	}
}

func TestRunWithRetry_LeavesUncommittedWhenDeadLetterFails(t *testing.T) {
	dlq := &fakeWriter{err: errors.New("kafka down")}
	p := newTestProcessor(dlq)

	commit := p.runWithRetry(context.Background(), []byte("x"), func(context.Context, []byte) error {
		return errors.New("boom")
	})

	if commit {
		t.Fatal("want commit=false when the dead-letter write itself fails (redeliver, don't drop)")
	}
}
