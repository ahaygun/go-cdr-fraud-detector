package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var cdrProduced = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdr_produced_total",
	Help: "Total CDR events produced onto cdr.raw.",
})
