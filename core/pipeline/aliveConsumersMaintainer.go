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
	BurrowURL    string
	ProduceQueue chan string
	CountService *module.CountService
	Logger       *zap.Logger

	clusterConsumerMap *util.SyncNestedMap
}

func (acm *AliveConsumersMaintainer) Start() {
	defer acm.Logger.Sync()

	acm.clusterConsumerMap = &util.SyncNestedMap{}
	acm.clusterConsumerMap.Init()

	contextProvider := util.ContextProvider{}
	contextProvider.Init("config/config.json")
	blacklist := contextProvider.GetBlacklist()

	for {
		clusters, clusterLink := getClusters(acm.BurrowURL)
		if clusters == nil {
			// Burrow server is not ready
			acm.Logger.Info("Burrow server not ready.")
			time.Sleep(1 * time.Minute)
			continue
		}
		for _, cluster := range clusters.([]interface{}) {
			clusterString := cluster.(string)
			consumersSet := acm.clusterConsumerMap.GetChild(clusterString, make(map[string]interface{})).(map[string]interface{})

			acm.clusterConsumerMap.SetLock(clusterString)

			consumers, consumersLink := getConsumers(clusterLink, clusterString)
			fmt.Println(consumers, consumersLink)

			// create new consumer handler if it does not exist.
			for _, consumer := range consumers.([]interface{}) {
				consumerString := consumer.(string)
				if _, ok := consumersSet[consumerString]; !ok {
					// A new consumer found, need to: 1. create new thread 2. put it into map.
					consumersSet[consumerString] = true
					if isInBlacklist, _ := regexp.MatchString(blacklist, consumerString); isInBlacklist {
						// if consumer name is in blacklist, put it in map and
						// skip initiating its consumer handler.
						acm.Logger.Info("the current consumer is in blacklist",
							zap.String("consumer", consumerString),
						)
						continue
					}
					consumerHandler := &ConsumerHandler{
						ProduceQueue:       acm.ProduceQueue,
						CountService:       acm.CountService,
						ClusterConsumerMap: acm.clusterConsumerMap,
						Logger: util.GetLogger().With(
							zap.String("module", "consumerHandler"),
						),
					}
					consumerHandler.Init(consumersLink, consumerString, clusterString)
					go consumerHandler.Start()
					acm.Logger.Info("create a new consumer handler",
						zap.String("consumer", consumerString),
						zap.String("cluster", clusterString),
					)
				}
			}

			acm.clusterConsumerMap.ReleaseLock(clusterString)
		}
		// AliveConsumerMaintainer refresh its alive Consumers list every 5 minutes.
		time.Sleep(5 * time.Minute)
	}
}

func (acm *AliveConsumersMaintainer) Stop() error {
	return nil
}
