// Package tariff holds the static destination-tariff catalog (seed reference
// data). The subscriber-service serves it over gRPC; the fraud service uses it
// to accrue a subscriber's spend on premium/international destinations (IRSF).
package tariff

import "strings"

// Tariff is the cost of calling a destination, matched by dialing prefix.
type Tariff struct {
	Prefix     string
	RatePerMin float64
	Premium    bool
	Name       string
}

// Catalog maps dialing prefixes to tariffs. Premium (revenue-share) prefixes
// are the ones IRSF fraud abuses.
var Catalog = []Tariff{
	{"+90", 0.10, false, "Turkey (domestic)"},
	{"+1", 0.40, false, "North America"},
	{"+44", 0.50, false, "United Kingdom"},
	{"+49", 0.50, false, "Germany"},
	{"+881", 12.00, true, "Global Mobile Satellite (premium)"},
	{"+882", 9.00, true, "International Networks (premium)"},
	{"+883", 9.00, true, "International Networks (premium)"},
	{"+900", 8.00, true, "Premium-rate service"},
}

// fallback is used for destinations that match no known prefix.
var fallback = Tariff{Prefix: "", RatePerMin: 1.00, Premium: false, Name: "International (other)"}

// Lookup returns the tariff for a destination MSISDN by longest-prefix match.
func Lookup(msisdn string) Tariff {
	best := fallback
	for _, t := range Catalog {
		if strings.HasPrefix(msisdn, t.Prefix) && len(t.Prefix) > len(best.Prefix) {
			best = t
		}
	}
	return best
}
