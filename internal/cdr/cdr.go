// Package cdr defines the event schema shared across services: the CDR record
// that flows on cdr.raw and the FraudAlert that flows on cdr.fraud.alert.
// JSON is the wire format for now — simple and readable; we can move to
// protobuf later if the schema or throughput calls for it.
package cdr

import (
	"encoding/json"
	"time"
)

// Kafka topic names and rule identifiers, kept in one place.
const (
	TopicRaw   = "cdr.raw"
	TopicAlert = "cdr.fraud.alert"

	RuleVelocity         = "velocity"
	RuleImpossibleTravel = "impossible_travel"
	RuleIRSF             = "irsf"
)

type CallType string

const (
	Voice CallType = "VOICE"
	SMS   CallType = "SMS"
	Data  CallType = "DATA"
)

// CDR is a single Call Detail Record — one call/SMS/data session.
type CDR struct {
	RecordID     string    `json:"record_id"`
	CallerMSISDN string    `json:"caller_msisdn"`
	CalleeMSISDN string    `json:"callee_msisdn"`
	StartTime    time.Time `json:"start_time"`
	DurationSec  int       `json:"duration_sec"`
	CellID       string    `json:"cell_id"`
	CallType     CallType  `json:"call_type"`
	Bytes        int64     `json:"bytes,omitempty"`
	Termination  string    `json:"termination"`
}

// FraudAlert is emitted on cdr.fraud.alert when a rule flags a record.
type FraudAlert struct {
	RecordID     string    `json:"record_id"`
	CallerMSISDN string    `json:"caller_msisdn"`
	Rule         string    `json:"rule"`
	Score        float64   `json:"score"`
	Evidence     string    `json:"evidence"`
	DetectedAt   time.Time `json:"detected_at"`
}

func (c CDR) Marshal() ([]byte, error) { return json.Marshal(c) }

func UnmarshalCDR(b []byte) (CDR, error) {
	var c CDR
	err := json.Unmarshal(b, &c)
	return c, err
}

func (a FraudAlert) Marshal() ([]byte, error) { return json.Marshal(a) }

func UnmarshalAlert(b []byte) (FraudAlert, error) {
	var a FraudAlert
	err := json.Unmarshal(b, &a)
	return a, err
}
