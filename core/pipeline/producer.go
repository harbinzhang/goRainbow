package pipeline

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/module"

	"github.com/HarbinZhang/goRainbow/core/util"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer send metrics to Kafka
type Producer struct {
	ProduceQueue chan string
	CountService *module.CountService
	Logger       *zap.Logger
}

func (producer *Producer) Start() {
	defer producer.Logger.Sync()

	contextProvider := util.ContextProvider{}
	contextProvider.Init("config/config.json")
	postfix := contextProvider.GetPostfix()
	conf := contextProvider.GetConf()

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
					producer.Logger.Warn("Delivery failed",
						zap.String("topicPartition", ev.TopicPartition.String()),
					)
				} else {
					// fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					producer.Logger.Warn("Delivered message")
				}
			}
		}
	}()

	// rcsMetricsSent is for metrics level traffic, how many metrics sent to wavefront
	rcsMetricsSent := &util.RequestCounter{
		Name:         "metricsSent",
		Interval:     60 * time.Second,
		ProducerChan: producer.ProduceQueue,
		Postfix:      postfix,
	}
	rcsMetricsSent.Init()

	// message := "fjord.burrow.test3.python-consumer-1.BusinessEvent.0.maxLag 0.00 1541214139 source=192.168.3.169 data_center=slv dca_zone=local department=fjord planet=sbx888 service_name=porter_rainbow porter_tools=porter-rainbow"

	// Wait for message deliveries before shutting down
	go p.Flush(15 * 1000)

	// Produce messages to topic (asynchronously)
	topic := conf.Kafka.Topic

	env := os.Getenv("ENV")

	for message := range producer.ProduceQueue {
		go rcsMetricsSent.Increase(env)
		// fmt.Println(message)
		logger.Info("Produced to speed-racer: " + message)
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)
	}

}
