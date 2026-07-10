package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var alertsStored = promauto.NewCounter(prometheus.CounterOpts{
	Name: "fraud_alerts_stored_total",
	Help: "Total fraud alerts stored by read-api.",
})
