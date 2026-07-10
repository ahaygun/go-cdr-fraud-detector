package rules

import (
	"testing"
	"time"
)

func TestImpossibleTravelEvaluate(t *testing.T) {
	r := ImpossibleTravel{MaxSpeedKmh: 1000}

	tests := []struct {
		name          string
		distanceKm    float64
		dt            time.Duration
		wantTriggered bool
	}{
		{"same place", 0, time.Hour, false},
		{"plausible drive", 100, 2 * time.Hour, false},           // 50 km/h
		{"plausible flight", 900, time.Hour, false},              // 900 km/h < 1000
		{"impossible: far & fast", 5000, 30 * time.Minute, true}, // 10000 km/h
		{"impossible: no time elapsed", 5000, 0, true},           // floored to 1s
		{"impossible: sub-second", 5000, 10 * time.Millisecond, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			triggered, score, evidence := r.Evaluate(tc.distanceKm, tc.dt)
			if triggered != tc.wantTriggered {
				t.Fatalf("Evaluate(%.0f, %s) triggered = %v, want %v", tc.distanceKm, tc.dt, triggered, tc.wantTriggered)
			}
			if triggered {
				if score <= 1 {
					t.Errorf("triggered but score %.2f not > 1", score)
				}
				if evidence == "" {
					t.Error("triggered alert has empty evidence")
				}
			}
		})
	}
}
