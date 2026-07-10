package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// lastLoc is a subscriber's most recent known location, used by the
// impossible-travel rule. It is per-subscriber state kept in Redis; because
// events are partitioned by caller_msisdn, one consumer owns a subscriber and
// the get-then-set below is race-free.
type lastLoc struct {
	Lat float64   `json:"lat"`
	Lon float64   `json:"lon"`
	At  time.Time `json:"at"`
}

func getLastLocation(ctx context.Context, rdb *redis.Client, msisdn string) (lastLoc, bool, error) {
	s, err := rdb.Get(ctx, "lastloc:"+msisdn).Result()
	if errors.Is(err, redis.Nil) {
		return lastLoc{}, false, nil
	}
	if err != nil {
		return lastLoc{}, false, err
	}
	var l lastLoc
	if err := json.Unmarshal([]byte(s), &l); err != nil {
		return lastLoc{}, false, err
	}
	return l, true, nil
}

func setLastLocation(ctx context.Context, rdb *redis.Client, msisdn string, l lastLoc, ttl time.Duration) error {
	b, err := json.Marshal(l)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, "lastloc:"+msisdn, b, ttl).Err()
}
