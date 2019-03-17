package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/HarbinZhang/goRainbow/core/pipeline"
	"github.com/HarbinZhang/goRainbow/core/utils"
)

func main() {
	const link string = "http://127.0.0.1:8000/v3/kafka"

	const LagQueueSize int = 1000
	const ProduceQueueSize int = 9000

	// Queue init
	produceQueue := make(chan string, ProduceQueueSize)

	// Preapre rcs for total metrics traffic.
	// Using it for health_check
	rcsTotal := &utils.RequestCountService{
		Name:         "totalMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
	}
	rcsTotal.Init()

	// Prepare pipeline routines
	pipeline.PrepareLogger()
	go pipeline.AliveConsumersMaintainer(link, produceQueue, rcsTotal)
	go pipeline.AliveTopicsMaintainer(link, produceQueue)
	go pipeline.Producer(produceQueue)

	// health_check server
	healthCheckHandler := utils.HealthChecker(rcsTotal)
	http.HandleFunc("/health_check", healthCheckHandler)
	http.ListenAndServe(":7099", nil)

	fmt.Println("server exited")
	close(produceQueue)
}
