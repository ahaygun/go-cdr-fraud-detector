package main

import (
	"errors"
	"testing"
	"time"
)

func TestRefCache_HitMissTTL(t *testing.T) {
	c := newRefCache[int](time.Minute)
	base := time.Unix(0, 0)
	calls := 0
	load := func(v int) func() (int, error) {
		return func() (int, error) { calls++; return v, nil }
	}

	// miss → loads and caches
	if got, err := c.get("k", base, load(42)); err != nil || got != 42 {
		t.Fatalf("miss: got %d err %v, want 42", got, err)
	}
	if calls != 1 {
		t.Fatalf("miss should load once, calls=%d", calls)
	}

	// hit within TTL → returns cached value, does NOT reload
	if got, _ := c.get("k", base.Add(30*time.Second), load(99)); got != 42 {
		t.Fatalf("hit should return cached 42, got %d", got)
	}
	if calls != 1 {
		t.Fatalf("hit should not reload, calls=%d", calls)
	}

	// past TTL → reloads
	if got, _ := c.get("k", base.Add(2*time.Minute), load(7)); got != 7 {
		t.Fatalf("expired should reload 7, got %d", got)
	}
	if calls != 2 {
		t.Fatalf("expired should reload once more, calls=%d", calls)
	}
}

func TestRefCache_ErrorNotCached(t *testing.T) {
	c := newRefCache[int](time.Minute)
	base := time.Unix(0, 0)
	boom := errors.New("enrichment down")

	// a load error propagates and is NOT cached (transient gRPC failure)
	if _, err := c.get("k", base, func() (int, error) { return 0, boom }); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	// next lookup retries the loader and can succeed
	if got, err := c.get("k", base, func() (int, error) { return 5, nil }); err != nil || got != 5 {
		t.Fatalf("after error the next load should work: got %d err %v", got, err)
	}
}
