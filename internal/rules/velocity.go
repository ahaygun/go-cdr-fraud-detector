// Package rules holds the fraud detection rules. Each rule is a small, pure
// decision so it can be unit-tested in isolation; the stateful counting (the
// Redis sliding window) lives in the service and feeds these functions.
package rules

import "fmt"

// Velocity flags an MSISDN that places too many calls within a time window —
// the classic signature of SIM-box / high-volume abuse.
type Velocity struct {
	WindowSeconds int
	Threshold     int
}

// Evaluate turns an observed call count (within the window) into a verdict.
// score is count/threshold (>= 1 when triggered); evidence is human-readable.
func (v Velocity) Evaluate(count int) (triggered bool, score float64, evidence string) {
	if v.Threshold <= 0 || count < v.Threshold {
		return false, 0, ""
	}
	score = float64(count) / float64(v.Threshold)
	evidence = fmt.Sprintf("%d calls within %ds (threshold %d)", count, v.WindowSeconds, v.Threshold)
	return true, score, evidence
}
