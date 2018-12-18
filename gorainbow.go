package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/HarbinZhang/goRainbow/config"
	"github.com/HarbinZhang/goRainbow/core"
)

func main() {
	const link string = "http://127.0.0.1:8000/v3/kafka"
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
	lagStatusQueue := make(chan config.LagStatus, LagQueueSize)
	produceQueue := make(chan string, ProduceQueueSize)

	// Preapre rcs for total metrics traffic.
	// Using it for health_check
	rcsTotal := &core.RequestCountService{
		Name:         "totalMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
	}
	rcsTotal.Init()

	go core.AliveConsumersMaintainer(link, lagStatusQueue)
	go core.Translator(lagStatusQueue, produceQueue, rcsTotal)
	go core.Produce(produceQueue)

	// health_check server
	healthCheckHandler := core.HealthChecker(rcsTotal)
	http.HandleFunc("/health_check", healthCheckHandler)
	http.ListenAndServe(":7099", nil)
	fmt.Println("server exited")
}
