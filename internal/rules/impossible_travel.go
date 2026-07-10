package rules

import (
	"fmt"
	"time"
)

// ImpossibleTravel flags a subscriber whose two consecutive calls are separated
// by a distance that could not be covered in the elapsed time — the signature
// of a cloned SIM or a hijacked account placing calls from two places at once.
type ImpossibleTravel struct {
	MaxSpeedKmh float64
}

// Evaluate reports whether covering distanceKm in dt implies an impossible
// speed. score is the implied speed / max; evidence is human-readable.
func (r ImpossibleTravel) Evaluate(distanceKm float64, dt time.Duration) (triggered bool, score float64, evidence string) {
	if distanceKm <= 0 || r.MaxSpeedKmh <= 0 {
		return false, 0, ""
	}

	// Floor the interval so near-simultaneous calls yield a large but finite
	// speed (avoids divide-by-zero and non-serializable +Inf scores).
	if dt < time.Second {
		dt = time.Second
	}

	speed := distanceKm / dt.Hours()
	if speed <= r.MaxSpeedKmh {
		return false, 0, ""
	}
	evidence = fmt.Sprintf("%.0f km in %s → %.0f km/h (max %.0f)",
		distanceKm, dt.Round(time.Second), speed, r.MaxSpeedKmh)
	return true, speed / r.MaxSpeedKmh, evidence
}
