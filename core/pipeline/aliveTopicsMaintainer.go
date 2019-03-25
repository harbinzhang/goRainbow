package pipeline

import (
	"time"

	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/module"

	"github.com/HarbinZhang/goRainbow/core/util"
)

// AliveTopicsMaintainer is a maintainer for alive topics
// It checks Burrow periodically to see if there is a new topic, then creates a new thread for this topic.
type AliveTopicsMaintainer struct {
	BurrowURL    string
	ProduceQueue chan string
	CountService *module.CountService
	Logger       *zap.Logger

	clusterTopicMap *util.SyncNestedMap
}

func (atm *AliveTopicsMaintainer) Start() {
	defer atm.Logger.Sync()

	contextProvider := util.ContextProvider{}
	contextProvider.Init("config/config.json")
	postfix := contextProvider.GetPostfix()

	atm.clusterTopicMap = &util.SyncNestedMap{}
	atm.clusterTopicMap.Init()

	for {
		clusters, clusterLink := getClusters(atm.BurrowURL)
		if clusters == nil {
			// Burrow server is not ready
			atm.Logger.Info("Burrow server not ready")
			time.Sleep(1 * time.Minute)
			continue
		}
		for _, cluster := range clusters.([]interface{}) {
			clusterString := cluster.(string)
			topicsSet := atm.clusterTopicMap.GetChild(clusterString, make(map[string]interface{})).(map[string]interface{})

			atm.clusterTopicMap.SetLock(clusterString)

			topics, topicsLink := getTopics(clusterLink, clusterString)

			// create new go routine if consumer not exists.
			for _, topic := range topics.([]interface{}) {
				topicString := topic.(string)
				if _, ok := topicsSet[topicString]; !ok {
					// A new consumer found, need to 1. create new thread 2. put it into map.
					topicsSet[topicString] = true
					topicHandler := &TopicHandler{
						ProduceQueue:    atm.ProduceQueue,
						ClusterTopicMap: atm.clusterTopicMap,
						CountService:    atm.CountService,
						Logger: atm.Logger.With(
							zap.String("name", "topicHandler"),
						),
					}
					topicHandler.Init(topicsLink, topicString, clusterString, postfix)
					go topicHandler.Start()
					atm.Logger.Info("create a new topic handler",
						zap.String("topic", topicString),
						zap.String("cluster", clusterString),
					)
				}
			}
			atm.clusterTopicMap.ReleaseLock(clusterString)
		}
		time.Sleep(5 * time.Minute)
	}
}

func (atm *AliveTopicsMaintainer) Stop() {

}

func getTopics(link string, cluster string) (interface{}, string) {
	topicsLink := link + cluster + "/topic"
	return getHTTPSubSlice(topicsLink, "topics"), topicsLink + "/"
}
