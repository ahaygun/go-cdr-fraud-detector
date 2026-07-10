package rules

import "testing"

func TestIRSFEvaluate(t *testing.T) {
	r := IRSF{WindowSeconds: 300, SpendThreshold: 50}

	tests := []struct {
		name          string
		spend         float64
		wantTriggered bool
	}{
		{"no spend", 0, false},
		{"below threshold", 49.99, false},
		{"at threshold", 50, true},
		{"spike", 480, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			triggered, score, evidence := r.Evaluate(tc.spend)
			if triggered != tc.wantTriggered {
				t.Fatalf("Evaluate(%.2f) triggered = %v, want %v", tc.spend, triggered, tc.wantTriggered)
			}
			if triggered {
				if score < 1 {
					t.Errorf("triggered but score %.2f < 1", score)
				}
				if evidence == "" {
					t.Error("triggered alert has empty evidence")
				}
			}
		})
	}
}

func TestIRSFZeroThresholdNeverTriggers(t *testing.T) {
	r := IRSF{WindowSeconds: 300, SpendThreshold: 0}
	if triggered, _, _ := r.Evaluate(10000); triggered {
		t.Error("a zero/invalid threshold must never trigger")
	}
}
