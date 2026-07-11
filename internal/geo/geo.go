// Package geo holds the static cell-tower catalog (seed reference data) and a
// great-circle distance helper. The subscriber-service serves these cells over
// gRPC; the generator draws cell IDs from the same catalog so the two agree.
package geo

import "math"

// Cell is a cell tower with a geographic location.
type Cell struct {
	ID   string
	Lat  float64
	Lon  float64
	Name string
}

// Catalog is the known set of cell towers, spread across cities so that an
// injected "impossible-travel" jump (e.g. Istanbul → Tokyo in seconds) is
// clearly detectable while normal same-city traffic is not.
var Catalog = []Cell{
	{"CELL-IST-01", 41.0082, 28.9784, "Istanbul"},
	{"CELL-IST-02", 40.9930, 29.0300, "Istanbul-Kadikoy"},
	{"CELL-ANK-01", 39.9334, 32.8597, "Ankara"},
	{"CELL-IZM-01", 38.4237, 27.1428, "Izmir"},
	{"CELL-ANT-01", 36.8969, 30.7133, "Antalya"},
	{"CELL-BUR-01", 40.1826, 29.0665, "Bursa"},
	{"CELL-KON-01", 37.8746, 32.4932, "Konya"},
	{"CELL-ADA-01", 37.0000, 35.3213, "Adana"},
	{"CELL-GAZ-01", 37.0662, 37.3833, "Gaziantep"},
	{"CELL-SAM-01", 41.2867, 36.3300, "Samsun"},
	{"CELL-TRA-01", 41.0027, 39.7168, "Trabzon"},
	{"CELL-DIY-01", 37.9144, 40.2306, "Diyarbakir"},
	{"CELL-ERZ-01", 39.9043, 41.2679, "Erzurum"},
	{"CELL-BER-01", 52.5200, 13.4050, "Berlin"},
	{"CELL-LON-01", 51.5074, -0.1278, "London"},
	{"CELL-PAR-01", 48.8566, 2.3522, "Paris"},
	{"CELL-DXB-01", 25.2048, 55.2708, "Dubai"},
	{"CELL-NYC-01", 40.7128, -74.0060, "New York"},
	{"CELL-TYO-01", 35.6762, 139.6503, "Tokyo"},
	{"CELL-SYD-01", -33.8688, 151.2093, "Sydney"},
}

var byID = func() map[string]Cell {
	m := make(map[string]Cell, len(Catalog))
	for _, c := range Catalog {
		m[c.ID] = c
	}
	return m
}()

// Lookup returns the cell with the given id.
func Lookup(id string) (Cell, bool) {
	c, ok := byID[id]
	return c, ok
}

// HaversineKm returns the great-circle distance in kilometers between two
// (lat, lon) points in degrees.
func HaversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	dLat := rad(lat2 - lat1)
	dLon := rad(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(rad(lat1))*math.Cos(rad(lat2))*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusKm * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func rad(deg float64) float64 { return deg * math.Pi / 180 }
