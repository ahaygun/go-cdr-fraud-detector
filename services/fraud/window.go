package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// slidingWindowCount records this call in the caller's sliding window and
// returns how many distinct calls fall within the last windowMs.
//
// The ZSET member is the record_id, which makes the count IDEMPOTENT: a
// redelivered record just updates its own score and is still counted once.
func slidingWindowCount(ctx context.Context, rdb *redis.Client, key string, eventMs, windowMs int64, recordID string) (int, error) {
	pipe := rdb.TxPipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(eventMs), Member: recordID})
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", eventMs-windowMs)) // drop entries older than the window
	card := pipe.ZCard(ctx, key)
	pipe.Expire(ctx, key, time.Duration(windowMs*2)*time.Millisecond) // self-clean idle keys
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return int(card.Val()), nil
}

// seen / markSeen give at-least-once delivery an idempotent skip: a record we
// already fully handled is not processed (or re-alerted) again on redelivery.
func seen(ctx context.Context, rdb *redis.Client, recordID string) (bool, error) {
	n, err := rdb.Exists(ctx, "seen:"+recordID).Result()
	return n > 0, err
}

func markSeen(ctx context.Context, rdb *redis.Client, recordID string, ttl time.Duration) error {
	return rdb.Set(ctx, "seen:"+recordID, 1, ttl).Err()
}
