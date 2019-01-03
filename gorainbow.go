package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/HarbinZhang/goRainbow/core/pipeline"
	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/utils"
)

func main() {
	const link string = "http://127.0.0.1:8000/v3/kafka"
	// const link string = "http://18.224.147.248:8000/v3/kafka"

	const LagQueueSize int = 1000
	const ProduceQueueSize int = 9000

	// log init
	f, err := os.OpenFile("rainbow_log", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	log.SetOutput(f)
	log.Println("Log setup finished.")

	// Queue init
	lagStatusQueue := make(chan protocol.LagStatus, LagQueueSize)
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
	go pipeline.AliveConsumersMaintainer(link, lagStatusQueue)
	go pipeline.AliveTopicsMaintainer(link, produceQueue)
	go pipeline.Translator(lagStatusQueue, produceQueue, rcsTotal)
	go pipeline.Produce(produceQueue)

	// health_check server
	healthCheckHandler := utils.HealthChecker(rcsTotal)
	http.HandleFunc("/health_check", healthCheckHandler)
	http.ListenAndServe(":7099", nil)
	fmt.Println("server exited")
}
