package main

import (
	"fmt"
	"net/http"

	"github.com/HarbinZhang/goRainbow/core/module"
	"github.com/HarbinZhang/goRainbow/core/util"
	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/pipeline"
)

func main() {
	const link string = "http://127.0.0.1:8000/v3/kafka"

	const LagQueueSize int = 1000
	const ProduceQueueSize int = 9000

	// Queue init
	produceQueue := make(chan string, ProduceQueueSize)

	// Prepare count service
	countService := &module.CountService{}
	countService.Init(produceQueue)

	//prepare logger
	logger := util.GetLogger()

	// Prepare pipeline routines
	aliveConsumersMaintainer := &pipeline.AliveConsumersMaintainer{
		BurrowURL:    link,
		ProduceQueue: produceQueue,
		CountService: countService,
		Logger: logger.With(
			zap.String("name", "aliveConsumersMaintainer"),
		),
	}

	aliveTopicsMaintainer := &pipeline.AliveTopicsMaintainer{
		BurrowURL:    link,
		ProduceQueue: produceQueue,
		CountService: countService,
		Logger: logger.With(
			zap.String("name", "aliveTopicsMaintainer"),
		),
	}

	go aliveConsumersMaintainer.Start()
	go aliveTopicsMaintainer.Start()

	go pipeline.Producer(produceQueue, countService)

	// health_check server
	healthCheckHandler := module.HealthChecker(countService)
	http.HandleFunc("/health_check", healthCheckHandler)
	http.ListenAndServe(":7099", nil)

	fmt.Println("server exited")
	close(produceQueue)
}
