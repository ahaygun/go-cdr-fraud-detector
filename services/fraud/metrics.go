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
	// End-to-end latency: from CDR production (StartTime) to fully processed.
	cdrLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cdr_processing_latency_seconds",
		Help:    "Latency from CDR production to fraud processing completion.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~8s
	})
)
