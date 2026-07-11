//go:build js && wasm

// Command playground compiles the project's real fraud-rule engine to
// WebAssembly so it runs in the browser. It reuses internal/rules, internal/geo
// and internal/tariff unchanged — the same Evaluate logic that runs in the
// Kafka pipeline (see services/fraud). Only two things are swapped for the
// browser: the per-subscriber state store (Redis in the live service) becomes
// in-memory maps, and reference enrichment (gRPC in the live service) is served
// from the local geo/tariff catalogs. It is a demo of the decision engine, not
// the distributed pipeline.
package main

import (
	"encoding/json"
	"syscall/js"
	"time"

	"github.com/ahaygun/go-cdr-fraud-detector/internal/cdr"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/geo"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/rules"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/tariff"
)

// engine mirrors services/fraud.processor, with in-memory state instead of
// Redis. The rules themselves are the project's real rules, untouched.
type engine struct {
	vel    rules.Velocity
	travel rules.ImpossibleTravel
	irsf   rules.IRSF

	windowMs     int64
	irsfWindowMs int64
	cooldownMs   int64

	// per-subscriber state (bounded by the generator's subscriber pool)
	velWin   map[string]map[string]int64     // caller -> record_id -> event ms   (sliding window)
	lastLoc  map[string]location             // caller -> last known cell location
	spendWin map[string]map[string]spendItem // caller -> record_id -> {ms, cost}  (spend window)
	alerted  map[string]int64                // "rule:caller" -> last-alert ms      (cooldown)
}

type location struct {
	lat, lon float64
	at       time.Time
}

type spendItem struct {
	ms   int64
	cost float64
}

func newEngine() *engine {
	return &engine{
		// same defaults as services/fraud (env-var defaults)
		vel:    rules.Velocity{WindowSeconds: 60, Threshold: 12},
		travel: rules.ImpossibleTravel{MaxSpeedKmh: 1000},
		irsf:   rules.IRSF{WindowSeconds: 300, SpendThreshold: 50},

		windowMs:     60 * 1000,
		irsfWindowMs: 300 * 1000,
		cooldownMs:   60 * 1000,

		velWin:   map[string]map[string]int64{},
		lastLoc:  map[string]location{},
		spendWin: map[string]map[string]spendItem{},
		alerted:  map[string]int64{},
	}
}

// configure updates the live thresholds from the UI sliders.
func (e *engine) configure(velThreshold, travelKmh, irsfSpend float64) {
	e.vel.Threshold = int(velThreshold)
	e.travel.MaxSpeedKmh = travelKmh
	e.irsf.SpendThreshold = irsfSpend
}

// score runs the three rules over one record, exactly as
// services/fraud.processor.handle does: velocity, impossible-travel, IRSF.
func (e *engine) score(rec cdr.CDR) []cdr.FraudAlert {
	var alerts []cdr.FraudAlert
	eventMs := rec.StartTime.UnixMilli()

	// ── velocity: Redis sliding-window count → in-memory ──
	if count := e.velocityCount(rec.CallerMSISDN, rec.RecordID, eventMs); true {
		if triggered, score, evidence := e.vel.Evaluate(count); triggered {
			if a, ok := e.maybeEmit(rec, cdr.RuleVelocity, score, evidence, eventMs); ok {
				alerts = append(alerts, a)
			}
		}
	}

	// ── impossible-travel: gRPC cell enrichment → geo.Lookup; last-loc state → in-memory ──
	if cell, ok := geo.Lookup(rec.CellID); ok {
		current := location{lat: cell.Lat, lon: cell.Lon, at: rec.StartTime}
		if last, seen := e.lastLoc[rec.CallerMSISDN]; seen {
			dist := geo.HaversineKm(last.lat, last.lon, current.lat, current.lon)
			dt := current.at.Sub(last.at)
			if dt < 0 {
				dt = -dt
			}
			if triggered, score, evidence := e.travel.Evaluate(dist, dt); triggered {
				if a, ok := e.maybeEmit(rec, cdr.RuleImpossibleTravel, score, evidence, eventMs); ok {
					alerts = append(alerts, a)
				}
			}
		}
		e.lastLoc[rec.CallerMSISDN] = current // emit before advancing state
	}

	// ── IRSF: gRPC tariff enrichment → tariff.Lookup; spend-window state → in-memory ──
	if t := tariff.Lookup(rec.CalleeMSISDN); t.Premium {
		cost := float64(rec.DurationSec) / 60.0 * t.RatePerMin
		spend := e.spendInWindow(rec.CallerMSISDN, rec.RecordID, eventMs, cost)
		if triggered, score, evidence := e.irsf.Evaluate(spend); triggered {
			if a, ok := e.maybeEmit(rec, cdr.RuleIRSF, score, evidence, eventMs); ok {
				alerts = append(alerts, a)
			}
		}
	}

	return alerts
}

// velocityCount mirrors window.go slidingWindowCount: add this record, drop
// entries older than the window, return how many remain. Keyed by record_id so
// a redelivered record cannot inflate the count.
func (e *engine) velocityCount(caller, recordID string, eventMs int64) int {
	w := e.velWin[caller]
	if w == nil {
		w = map[string]int64{}
		e.velWin[caller] = w
	}
	w[recordID] = eventMs
	cutoff := eventMs - e.windowMs
	for id, ms := range w {
		if ms <= cutoff {
			delete(w, id)
		}
	}
	return len(w)
}

// spendInWindow mirrors spend.go premiumSpendInWindow: add this record's cost,
// drop entries older than the window, return the sum of what remains.
func (e *engine) spendInWindow(caller, recordID string, eventMs int64, cost float64) float64 {
	w := e.spendWin[caller]
	if w == nil {
		w = map[string]spendItem{}
		e.spendWin[caller] = w
	}
	w[recordID] = spendItem{ms: eventMs, cost: cost}
	cutoff := eventMs - e.irsfWindowMs
	var sum float64
	for id, it := range w {
		if it.ms <= cutoff {
			delete(w, id)
			continue
		}
		sum += it.cost
	}
	return sum
}

// maybeEmit mirrors processor.maybeEmit: at most one alert per (rule,
// subscriber) within the cooldown window, so a burst yields one alert.
func (e *engine) maybeEmit(rec cdr.CDR, rule string, score float64, evidence string, eventMs int64) (cdr.FraudAlert, bool) {
	key := rule + ":" + rec.CallerMSISDN
	if last, ok := e.alerted[key]; ok && eventMs-last < e.cooldownMs {
		return cdr.FraudAlert{}, false
	}
	e.alerted[key] = eventMs
	return cdr.FraudAlert{
		RecordID:     rec.RecordID,
		CallerMSISDN: rec.CallerMSISDN,
		Rule:         rule,
		Score:        score,
		Evidence:     evidence,
		DetectedAt:   time.Now().UTC(),
	}, true
}

var eng = newEngine()

func main() {
	// cdrConfigure(velThreshold, travelKmh, irsfSpend) — update thresholds live.
	js.Global().Set("cdrConfigure", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 3 {
			eng.configure(args[0].Float(), args[1].Float(), args[2].Float())
		}
		return nil
	}))

	// cdrScore(cdrJSON) -> alertsJSON — run the real rules over one record.
	js.Global().Set("cdrScore", js.FuncOf(func(_ js.Value, args []js.Value) any {
		var rec cdr.CDR
		if err := json.Unmarshal([]byte(args[0].String()), &rec); err != nil {
			return "[]"
		}
		alerts := eng.score(rec)
		if len(alerts) == 0 {
			return "[]"
		}
		b, err := json.Marshal(alerts)
		if err != nil {
			return "[]"
		}
		return string(b)
	}))

	// cdrReset() — clear all in-memory state (fresh run).
	js.Global().Set("cdrReset", js.FuncOf(func(_ js.Value, _ []js.Value) any {
		eng = newEngine()
		return nil
	}))

	// Signal the page that the engine is ready to receive records.
	if cb := js.Global().Get("onWasmReady"); cb.Type() == js.TypeFunction {
		cb.Invoke()
	}

	select {} // keep the Go runtime alive so the exported callbacks stay callable
}
