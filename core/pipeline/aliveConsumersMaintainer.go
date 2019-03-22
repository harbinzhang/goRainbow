package pipeline

import (
	"fmt"
	"regexp"
	"time"

	"github.com/HarbinZhang/goRainbow/core/module"

	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/util"
)

// AliveConsumersMaintainer is a maintainer for alive consumers
// It checks Burrow periodically to see if there is a new consumer, then creates a new thread for this consumer.
type AliveConsumersMaintainer struct {
	BurrowURL     string
	ProducerQueue chan string
	CountService  *module.CountService
	Logger        *zap.Logger

	clusterConsumerMap *util.SyncNestedMap
}

func (acm *AliveConsumersMaintainer) Start() {
	defer logger.Sync()

	clusterConsumerMap := &util.SyncNestedMap{}
	clusterConsumerMap.Init()

	contextProvider := util.ContextProvider{}
	contextProvider.Init("config/config.json")
	blacklist := contextProvider.GetBlacklist()

	for {
		clusters, clusterLink := getClusters(acm.BurrowURL)
		if clusters == nil {
			// Burrow server is not ready
			time.Sleep(1 * time.Minute)
			continue
		}
		for _, cluster := range clusters.([]interface{}) {
			clusterString := cluster.(string)
			consumersSet := clusterConsumerMap.GetChild(clusterString, make(map[string]interface{})).(map[string]interface{})

			clusterConsumerMap.SetLock(clusterString)

			consumers, consumersLink := getConsumers(clusterLink, clusterString)
			fmt.Println(consumers, consumersLink)

			// create new go routine if consumer not exists.
			for _, consumer := range consumers.([]interface{}) {
				consumerString := consumer.(string)
				if _, ok := consumersSet[consumerString]; !ok {
					// A new consumer found, need to: 1. create new thread 2. put it into map.
					consumersSet[consumerString] = true
					if isInBlacklist, _ := regexp.MatchString(blacklist, consumerString); isInBlacklist {
						// if consumer name is in blacklist, put it in map and
						// skip initiating its consumer handler.
						continue
					}
					consumerHandler := &ConsumerHandler{
						ProduceQueue:       acm.ProducerQueue,
						CountService:       acm.CountService,
						Logger:             acm.Logger,
						ClusterConsumerMap: acm.clusterConsumerMap,
					}
					go consumerHandler.Start()
				}
			}

			clusterConsumerMap.ReleaseLock(clusterString)
		}
		time.Sleep(5 * time.Minute)
	}
}

func (acm *AliveConsumersMaintainer) Stop() {

}
