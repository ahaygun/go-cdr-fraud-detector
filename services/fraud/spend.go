package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// premiumSpendInWindow adds this premium call's cost to the subscriber's spend
// window and returns the total premium spend within it.
//
// Like the velocity window, this is IDEMPOTENT: the ZSET member encodes the
// record_id (and its cost), so a redelivered record updates its own entry
// rather than double-counting the spend.
func premiumSpendInWindow(ctx context.Context, rdb *redis.Client, key string, eventMs, windowMs int64, recordID string, cost float64) (float64, error) {
	member := recordID + "|" + strconv.FormatFloat(cost, 'f', 4, 64)

	pipe := rdb.TxPipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(eventMs), Member: member})
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", eventMs-windowMs)) // drop entries older than the window
	rangeCmd := pipe.ZRange(ctx, key, 0, -1)                                  // members still in the window (after trim)
	pipe.Expire(ctx, key, time.Duration(windowMs*2)*time.Millisecond)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	var sum float64
	for _, m := range rangeCmd.Val() {
		if i := strings.LastIndex(m, "|"); i >= 0 {
			if c, err := strconv.ParseFloat(m[i+1:], 64); err == nil {
				sum += c
			}
		}
	}
	return sum, nil
}
