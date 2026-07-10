package rules

import "fmt"

// IRSF (International Revenue Share Fraud) flags a subscriber whose spend on
// premium/international destinations spikes within a window — the signature of
// a fraudster pumping traffic to revenue-share numbers.
type IRSF struct {
	WindowSeconds  int
	SpendThreshold float64
}

// Evaluate reports whether the accrued premium spend in the window trips the
// rule. score is spend / threshold; evidence is human-readable.
func (r IRSF) Evaluate(premiumSpend float64) (triggered bool, score float64, evidence string) {
	if r.SpendThreshold <= 0 || premiumSpend < r.SpendThreshold {
		return false, 0, ""
	}
	evidence = fmt.Sprintf("premium spend %.2f within %ds (threshold %.2f)",
		premiumSpend, r.WindowSeconds, r.SpendThreshold)
	return true, premiumSpend / r.SpendThreshold, evidence
}
