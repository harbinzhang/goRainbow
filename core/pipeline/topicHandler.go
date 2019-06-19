package pipeline

import (
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/module"
	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/util"
)

// TopicHandler is a offset handler for topic.
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

// Init is a general init
func (th *TopicHandler) Init(topicLink string, topic string, cluster string, postfix string) {
	th.topicLink = topicLink
	th.topic = topic
	th.cluster = cluster
	th.postfix = postfix
}

// Start is a general start
func (th *TopicHandler) Start() {
	defer th.Logger.Sync()

	fmt.Println("New topic found: ", th.topicLink, th.topic)

	prefix := "fjord.burrow." + th.cluster + ".topic." + th.topic

	// Prepare producer side offset change per minute
	th.oom = &module.OwnerOffsetMoveHelper{
		CountService: th.CountService,
		ProduceQueue: th.ProduceQueue,
		Logger: util.GetLogger().With(
			zap.String("module", "topicOwnerOffsetMoveHelper"),
		),
	}
	th.oom.Init(prefix, th.postfix, th.cluster, "offsetRate")

	ticker := time.NewTicker(60 * time.Second)
	for {
		// check its topic offset from Burrow periodically
		<-ticker.C
		var topicOffset protocol.TopicOffset
		getHTTPStruct(th.topicLink+th.topic, &topicOffset)
		if topicOffset.Error {
			th.Logger.Warn("Get consumer /lag error",
				zap.String("message", topicOffset.Message),
				zap.Int64("timestamp", time.Now().Unix()),
			)
			break
		}

		go th.handleTopicOffset(topicOffset, prefix)

	}

	// snm.DeregisterChild(cluster, topic)
	// this can be deadlock in some extreme cases.
	th.ClusterTopicMap.SetLock(th.cluster)
	delete(th.ClusterTopicMap.GetChild(th.cluster, nil).(map[string]interface{}), th.topic)
	th.ClusterTopicMap.ReleaseLock(th.cluster)

	th.Logger.Warn("Topic is invalid, will stop handler",
		zap.String("topic", th.topic),
		zap.String("cluster", th.cluster),
		zap.Int64("timestamp", time.Now().Unix()),
	)
}

// Stop is a general stop
func (th *TopicHandler) Stop() error {
	return nil
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
