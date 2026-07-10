package rules

import "testing"

func TestVelocityEvaluate(t *testing.T) {
	v := Velocity{WindowSeconds: 60, Threshold: 10}

	tests := []struct {
		name          string
		count         int
		wantTriggered bool
		wantMinScore  float64
	}{
		{"zero calls", 0, false, 0},
		{"just below threshold", 9, false, 0},
		{"exactly at threshold", 10, true, 1.0},
		{"well above threshold", 25, true, 2.5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			triggered, score, evidence := v.Evaluate(tc.count)
			if triggered != tc.wantTriggered {
				t.Fatalf("Evaluate(%d) triggered = %v, want %v", tc.count, triggered, tc.wantTriggered)
			}
			if triggered {
				if score < tc.wantMinScore {
					t.Errorf("score = %.2f, want >= %.2f", score, tc.wantMinScore)
				}
				if evidence == "" {
					t.Error("triggered alert has empty evidence")
				}
			}
		})
	}
}

func TestVelocityZeroThresholdNeverTriggers(t *testing.T) {
	v := Velocity{WindowSeconds: 60, Threshold: 0}
	if triggered, _, _ := v.Evaluate(1000); triggered {
		t.Error("a zero/invalid threshold must never trigger")
	}
}
