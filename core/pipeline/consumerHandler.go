package pipeline

import (
	"fmt"
	"time"

	"github.com/HarbinZhang/goRainbow/core/module"
	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/util"
	"go.uber.org/zap"
)

type ConsumerHandler struct {
	ProduceQueue       chan string
	CountService       *module.CountService
	Logger             *zap.Logger
	ClusterConsumerMap *util.SyncNestedMap

	consumersLink string
	consumer      string
	cluster       string
}

func (ch *ConsumerHandler) Init(consumersLink string, consumer string, cluster string) {
	ch.consumersLink = consumersLink
	ch.consumer = consumer
	ch.cluster = cluster
}

func (ch *ConsumerHandler) Start() {
	defer ch.Logger.Sync()

	fmt.Println("New consumer found: ", ch.consumersLink, ch.consumer)
	var lagStatus protocol.LagStatus

	lagStatusQueue := make(chan protocol.LagStatus)

	ticker := time.NewTicker(30 * time.Second)

	prefix := "fjord.burrow." + ch.cluster + "." + ch.consumer

	translator := &Translator{
		LagQueue:     lagStatusQueue,
		ProduceQueue: ch.ProduceQueue,
		CountService: ch.CountService,
		Logger: ch.Logger.With(
			zap.String("name", "Translator"),
		),
	}
	translator.Init(prefix, ch.cluster)
	go translator.Start()

	for {
		// check its ch.consumer lag from Burrow periodically
		<-ticker.C
		getHTTPStruct(ch.consumersLink+ch.consumer+"/lag", &lagStatus)
		if lagStatus.Error {
			ch.Logger.Warn("Get consumer /lag error",
				zap.String("message", lagStatus.Message),
			)
			break
		}
		// fmt.Println(lagStatus)
		lagStatusQueue <- lagStatus
	}

	// snm.DeregisterChild(cluster, ch.consumer)
	ch.ClusterConsumerMap.SetLock(ch.cluster)
	delete(ch.ClusterConsumerMap.GetChild(ch.cluster, nil).(map[string]interface{}), ch.consumer)
	ch.ClusterConsumerMap.ReleaseLock(ch.cluster)

	close(lagStatusQueue)
	ch.Logger.Warn("consumer is invalid, will stop handler.",
		zap.String("consumer", ch.consumer),
		zap.String("cluster", ch.cluster))
}

func (ch *ConsumerHandler) Stop() {

}
