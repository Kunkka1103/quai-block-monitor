package push

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"log"
)

func pushMetrics(pgURL, jobName string, value int) {
	log.Printf("Pushing metrics to Pushgateway at %s with job name %s...", pgURL, jobName)
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{})
	gauge.Set(float64(value))
	if err := push.New(pgURL, jobName).
		Collector(gauge).
		Push(); err != nil {
		log.Printf("Could not push to Pushgateway: %v", err)
	} else {
		log.Println("Pushed metrics successfully")
	}
}