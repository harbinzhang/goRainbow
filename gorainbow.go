package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/HarbinZhang/goRainbow/config"
	"github.com/HarbinZhang/goRainbow/core"
)

func main() {
	const link string = "localhost:8000/v3/kafka/"
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
	rcsTotal := &core.RequestCountService{
		Name:         "totalMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
	}
	rcsTotal.Init()

	clusters, clusterLink := core.GetClusters(link)

	go core.AliveConsumersMaintainer(clusters, clusterLink, lagStatusQueue)
	go core.Translator(lagStatusQueue, produceQueue, rcsTotal)
	go core.Produce(produceQueue)

	select {}
}
