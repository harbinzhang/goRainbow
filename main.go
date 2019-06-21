package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/HarbinZhang/goRainbow/core/module"
	"github.com/HarbinZhang/goRainbow/core/util"
	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/pipeline"
)

func main() {
	defer handleExit()

	const link string = "http://10.248.2.106:8000/v3/kafka"
	const ProduceQueueSize int = 9000

	os.Setenv("configPath", "config/config.json")

	// Queue init
	produceQueue := make(chan string, ProduceQueueSize)

	// Prepare count service
	countService := &module.CountService{ProduceQueue: produceQueue}
	countService.Start()

	//prepare logger
	logger := util.GetLogger()
	pipeline.PrepareLogger()

	// Prepare pipeline routines
	aliveConsumersMaintainer := &pipeline.AliveConsumersMaintainer{
		BurrowURL:    link,
		ProduceQueue: produceQueue,
		CountService: countService,
		Logger: logger.With(
			zap.String("module", "aliveConsumersMaintainer"),
		),
	}

	aliveTopicsMaintainer := &pipeline.AliveTopicsMaintainer{
		BurrowURL:    link,
		ProduceQueue: produceQueue,
		CountService: countService,
		Logger: logger.With(
			zap.String("module", "aliveTopicsMaintainer"),
		),
	}

	producer := &pipeline.Producer{
		ProduceQueue: produceQueue,
		CountService: countService,
		Logger: logger.With(
			zap.String("module", "producer"),
		),
	}

	go producer.Start()
	go aliveConsumersMaintainer.Start()
	go aliveTopicsMaintainer.Start()

	// health_check server
	healthCheckHandler := module.HealthChecker(countService)
	http.HandleFunc("/health_check", healthCheckHandler)
	http.ListenAndServe(":7099", nil)

	fmt.Println("goRainbow exited")

	countService.Stop()
	close(produceQueue)

	// exit cleanly
	os.Exit(0)
}

func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(string); ok {
			fmt.Fprintln(os.Stderr, exit, "Stopped goRainbow at", time.Now().Format("March 26, 2019 at 1:24pm (PST)"))
			os.Exit(1)
		}
		panic(e) // not an string, bubble up
	}
}
