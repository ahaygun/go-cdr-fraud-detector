package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	cdrProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdr_processed_total",
		Help: "Total CDR events processed by the fraud service.",
	})
	fraudAlerts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fraud_alerts_total",
		Help: "Total fraud alerts emitted, by rule.",
	}, []string{"rule"})
)
