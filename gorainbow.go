package main

import (
	"fmt"
	"net/http"

	"github.com/HarbinZhang/goRainbow/core/modules"

	"github.com/HarbinZhang/goRainbow/core/pipeline"
)

func main() {
	const link string = "http://127.0.0.1:8000/v3/kafka"

	const LagQueueSize int = 1000
	const ProduceQueueSize int = 9000

	// Queue init
	produceQueue := make(chan string, ProduceQueueSize)

	// Prepare count service
	countService := &modules.CountService{}
	countService.Init()

	// Prepare pipeline routines
	pipeline.PrepareLogger()
	go pipeline.AliveConsumersMaintainer(link, produceQueue, countService)
	go pipeline.AliveTopicsMaintainer(link, produceQueue, countService)
	go pipeline.Producer(produceQueue, countService)

	// health_check server
	healthCheckHandler := modules.HealthChecker(countService)
	http.HandleFunc("/health_check", healthCheckHandler)
	http.ListenAndServe(":7099", nil)

	fmt.Println("server exited")
	close(produceQueue)
}
