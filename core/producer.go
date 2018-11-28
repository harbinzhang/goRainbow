package core

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Produce message to kafka
func Produce(produceQueue chan string) {

	var conf config.Config
	configFile, _ := os.Open("config/config.json")
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	if err := decoder.Decode(&conf); err != nil {
		fmt.Println("Err conf for produce: ", err)
	}

	// Prepare tags
	dataCenter := "data_center=" + conf.Service.DataCenter
	department := "department=" + conf.Service.Department
	planet := "planet=" + conf.Service.Planet
	serviceName := "service_name=" + conf.Service.Name
	porterTools := "porter_tools=goRainbow"

	// Need to do what
	source := "source=192.168.3.169"
	dcaZone := "dca_zone=local"

	// postfix := "source=192.168.3.169 data_center=slv dca_zone=local department=fjord planet=sbx888 service_name=porter_rainbow porter_tools=porter-rainbow"
	postfix := strings.Join([]string{source, dataCenter, dcaZone, department, planet, serviceName, porterTools}, " ")

	kafkaConfig := kafka.ConfigMap{
		"batch.num.messages": 2000,
		"linger.ms":          1,
		"bootstrap.servers":  conf.Kafka.BrokerServers,
		// "buffer.memory=33554432"
		"socket.send.buffer.bytes": 1024000,
		// "reconnect.backoff.ms":     100,
		// "block.on.buffer.full=true"
		"retries":            6,
		"retry.backoff.ms":   100,
		"compression.type":   "gzip",
		"request.timeout.ms": 900000,
	}
	p, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					// fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// rcsMetricsSent is for metrics level traffic, how many metrics sent to wavefront
	rcsMetricsSent := &RequestCountService{
		name:         "metricsSent",
		interval:     60 * time.Second,
		producerChan: produceQueue,
		postfix:      postfix,
	}
	rcsMetricsSent.Init()

	// message := "fjord.burrow.test3.python-consumer-1.BusinessEvent.0.maxLag 0.00 1541214139 source=192.168.3.169 data_center=slv dca_zone=local department=fjord planet=sbx888 service_name=porter_rainbow porter_tools=porter-rainbow"

	// Wait for message deliveries before shutting down
	go p.Flush(15 * 1000)

	// Produce messages to topic (asynchronously)
	topic := conf.Kafka.Topic

	for message := range produceQueue {
		rcsMetricsSent.Increase("all")
		// fmt.Println(message)
		log.Println("Produced to speed-racer: " + message)
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)
	}

}
