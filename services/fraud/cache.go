package main

import (
	"sync"
	"time"
)

// refCache is a tiny cache-aside store for the static reference data the fraud
// service fetches from subscriber-service over gRPC (cell → geo, destination
// tariff). That data is static, so a short TTL keeps steady-state lookups off
// the wire — removing the pipeline's main bottleneck, the two synchronous gRPC
// calls per record — while still refreshing occasionally and surviving a brief
// enrichment outage on already-cached keys.
type refCache[V any] struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]cacheEntry[V]
}

type cacheEntry[V any] struct {
	val V
	at  time.Time
}

func newRefCache[V any](ttl time.Duration) *refCache[V] {
	return &refCache[V]{ttl: ttl, m: make(map[string]cacheEntry[V])}
}

// get returns the cached value if it is still fresh; otherwise it calls load,
// stores the result and returns it. A load error is returned uncached, so a
// transient gRPC failure is retried on the next lookup.
func (c *refCache[V]) get(key string, now time.Time, load func() (V, error)) (V, error) {
	c.mu.Lock()
	if e, ok := c.m[key]; ok && now.Sub(e.at) < c.ttl {
		c.mu.Unlock()
		return e.val, nil
	}
	c.mu.Unlock()

	v, err := load()
	if err != nil {
		var zero V
		return zero, err
	}

	c.mu.Lock()
	c.m[key] = cacheEntry[V]{val: v, at: now}
	c.mu.Unlock()
	return v, nil
}

// cellGeo / tariffInfo are the fields each rule needs from the enriched
// reference data — cached instead of the full gRPC reply.
type cellGeo struct {
	lat, lon float64
}

type tariffInfo struct {
	premium    bool
	ratePerMin float64
}
