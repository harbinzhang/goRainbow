package pipeline

import (
	"fmt"
	"strconv"
	"time"

	"github.com/HarbinZhang/goRainbow/core/module"
	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/util"
	"go.uber.org/zap"
)

type TopicHandler struct {
	ProduceQueue    chan string
	ClusterTopicMap *util.SyncNestedMap
	CountService    *module.CountService
	Logger          *zap.Logger

	topicLink string
	topic     string
	cluster   string
	postfix   string
	oom       *module.OwnerOffsetMoveHelper
}

func (th *TopicHandler) Init(topicLink string, topic string, cluster string, postfix string) {
	th.topicLink = topicLink
	th.topic = topic
	th.cluster = cluster
	th.postfix = postfix
}

func (th *TopicHandler) Start() {
	defer th.Logger.Sync()

	fmt.Println("New topic found: ", th.topicLink, th.topic)
	var topicOffset protocol.TopicOffset

	prefix := "fjord.burrow." + th.cluster + ".topic." + th.topic

	// Prepare producer side offset change per minute
	oom := &module.OwnerOffsetMoveHelper{CountService: th.CountService}
	oom.Init(th.ProduceQueue, prefix, th.postfix, th.cluster, "offsetRate")

	ticker := time.NewTicker(60 * time.Second)
	for {
		// check its topic offset from Burrow periodically
		<-ticker.C
		getHTTPStruct(th.topicLink+th.topic, &topicOffset)
		if topicOffset.Error {
			break
		}
		// fmt.Println(lagStatus)

		go th.handleTopicOffset(topicOffset, prefix)

	}

	// snm.DeregisterChild(cluster, topic)
	// this can be deadlock in some cases.
	th.ClusterTopicMap.SetLock(th.cluster)
	delete(th.ClusterTopicMap.GetChild(th.cluster, nil).(map[string]interface{}), th.topic)
	th.ClusterTopicMap.ReleaseLock(th.cluster)

	logger.Warn("Topic is invalid",
		zap.String("topic", th.topic),
		zap.String("cluster", th.cluster))
}

func (th *TopicHandler) handleTopicOffset(topicOffset protocol.TopicOffset, prefix string) {
	topicTag := "topic=" + th.topic
	for id, offset := range topicOffset.Offsets {
		timeString := strconv.FormatInt(time.Now().Unix(), 10)
		partitionIDTag := "partitionId=" + strconv.Itoa(id)
		th.oom.Update(th.topic+":"+strconv.Itoa(id), offset, time.Now().Unix())
		th.ProduceQueue <- combineInfo([]string{prefix, strconv.Itoa(id), "offset"},
			[]string{strconv.Itoa(offset), timeString, th.postfix, topicTag, partitionIDTag})
	}
}
