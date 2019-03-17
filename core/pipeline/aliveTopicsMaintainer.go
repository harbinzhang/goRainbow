package pipeline

import (
	"fmt"
	"strconv"
	"time"

	"github.com/HarbinZhang/goRainbow/core/modules"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/utils"
	"go.uber.org/zap"
)

// AliveTopicsMaintainer is a maintainer for alive topics
// It checks Burrow periodically to see if there is a new topic, then creates a new thread for this topic.
func AliveTopicsMaintainer(link string, produceQueue chan string, countService *modules.CountService) {

	defer logger.Sync()

	contextProvider := utils.ContextProvider{}
	contextProvider.Init("config/config.json")
	postfix := contextProvider.GetPostfix()

	clusterTopicMap := &utils.SyncNestedMap{}
	clusterTopicMap.Init()
	for {
		clusters, clusterLink := GetClusters(link)
		if clusters == nil {
			// Burrow server is not ready
			time.Sleep(1 * time.Minute)
			continue
		}
		for _, cluster := range clusters.([]interface{}) {
			clusterString := cluster.(string)
			topicsSet := clusterTopicMap.GetChild(clusterString, make(map[string]interface{})).(map[string]interface{})

			clusterTopicMap.SetLock(clusterString)

			topics, topicsLink := getTopics(clusterLink, clusterString)

			// create new go routine if consumer not exists.
			for _, topic := range topics.([]interface{}) {
				topicString := topic.(string)
				if _, ok := topicsSet[topicString]; !ok {
					// A new consumer found, need to 1. create new thread 2. put it into map.
					topicsSet[topicString] = true
					go newTopic(topicsLink, topicString, clusterString, produceQueue, clusterTopicMap, postfix)
				}
			}

			clusterTopicMap.ReleaseLock(clusterString)
		}
		time.Sleep(5 * time.Minute)
	}
}

// NewConsumerForLag is a thread to handle new found consumer
func newTopic(topicLink string, topic string, cluster string, produceQueue chan string, snm *utils.SyncNestedMap, postfix string) {
	fmt.Println("New topic found: ", topicLink, topic)
	var topicOffset protocol.TopicOffset

	prefix := "fjord.burrow." + cluster + ".topic." + topic

	ticker := time.NewTicker(60 * time.Second)
	for {
		// check its topic offset from Burrow periodically
		<-ticker.C
		HTTPGetStruct(topicLink+topic, &topicOffset)
		if topicOffset.Error {
			break
		}
		// fmt.Println(lagStatus)
		topicOffsetHandler(topicOffset, prefix, postfix+" topic="+topic, produceQueue)
	}

	// snm.DeregisterChild(cluster, topic)
	snm.SetLock(cluster)
	delete(snm.GetChild(cluster, nil).(map[string]interface{}), topic)
	snm.ReleaseLock(cluster)

	logger.Warn("Topic is invalid",
		zap.String("topic", topic),
		zap.String("cluster", cluster))
}

func getTopics(link string, cluster string) (interface{}, string) {
	topicsLink := link + cluster + "/topic"
	return HTTPGetSubSlice(topicsLink, "topics"), topicsLink + "/"
}

func topicOffsetHandler(topicOffset protocol.TopicOffset, prefix string, postfix string, produceQueue chan string) {
	for id, offset := range topicOffset.Offsets {
		time := strconv.FormatInt(time.Now().Unix(), 10)
		partitionIDTag := "partitionId=" + strconv.Itoa(id)
		produceQueue <- combineInfo([]string{prefix, strconv.Itoa(id), "offset"}, []string{strconv.Itoa(offset), time, postfix, partitionIDTag})
	}

}
