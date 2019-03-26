package pipeline

import (
	"strconv"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/core/module"
	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/util"
)

// Translator for message translate from struct to string
type Translator struct {
	LagQueue     <-chan protocol.LagStatus
	ProduceQueue chan<- string
	CountService *module.CountService
	Logger       *zap.Logger

	prefix  string
	env     string
	postfix string
	tsm     *util.TwinStateMachine
	oom     *module.OwnerOffsetMoveHelper
}

func (t *Translator) Init(prefix string, env string) {
	t.prefix = prefix
	t.env = env

	contextProvider := util.ContextProvider{}
	contextProvider.Init("config/config.json")
	t.postfix = contextProvider.GetPostfix()

	// Prepare metrics traffic control
	t.tsm = &util.TwinStateMachine{}
	t.tsm.Init()

	// Prepare consumer side offset change per minute
	t.oom = &module.OwnerOffsetMoveHelper{
		CountService: t.CountService,
		ProduceQueue: t.ProduceQueue,
		Logger: util.GetLogger().With(
			zap.String("module", "consumerOwnerOffsetMoveHelper"),
		),
	}
	t.oom.Init(t.prefix, t.postfix, t.env, "hosts")
}

func (t *Translator) Start() {
	defer t.Logger.Sync()

	for lag := range t.LagQueue {
		// if lag doesn't change, sends it per 60s. Otherwise 30s.
		shouldSendIt := t.tsm.Put(lag.Status.Cluster+lag.Status.Group, lag.Status.Totallag)
		if !shouldSendIt {
			continue
		}
		go t.parseInfo(lag)
	}

	t.Logger.Warn("translator exit",
		zap.String("prefix", t.prefix),
		zap.Int64("timestamp", time.Now().Unix()),
	)
}

func (t *Translator) parseInfo(lag protocol.LagStatus) {
	// lag is 0 or non-zero.
	// parse it into lower level(partitions, maxlag).
	cluster := lag.Status.Cluster
	group := lag.Status.Group
	totalLag := strconv.Itoa(lag.Status.Totallag)
	timestamp := getEpochTime()

	envTag := "env=" + cluster
	consumerTag := "consumer=" + group
	newPostfix := strings.Join([]string{timestamp, t.postfix, envTag, consumerTag}, " ")

	t.CountService.Increase("totalMessage", cluster)

	t.ProduceQueue <- combineInfo([]string{t.prefix, "totalLag"}, []string{totalLag, newPostfix})

	if totalLag != "0" {
		t.CountService.Increase("validMessage", cluster)
	}

	go t.parsePartitionInfo(lag.Status.Partitions, newPostfix)
	go t.parseMaxLagInfo(lag.Status.Maxlag, newPostfix)
}

func (t *Translator) parsePartitionInfo(partitions []protocol.Partition, postfix string) {
	for _, partition := range partitions {

		partitionID := strconv.Itoa(partition.Partition)
		currentLag := partition.CurrentLag

		owner := partition.Owner
		// it happens when info is invalid, skip this info.
		if owner == "" {
			t.Logger.Warn("owner invalid",
				zap.String("prefix", t.prefix),
				zap.Int("currentLag", currentLag),
				zap.String("partitionID", partitionID),
				zap.Int64("timestamp", time.Now().Unix()),
			)
			t.CountService.Increase("exception.ownerInvalid", t.env)
			return
		}

		topic := partition.Topic

		startOffset := strconv.Itoa(partition.Start.Offset)
		// startOffsetTimestamp := strconv.FormatInt(partition.Start.Timestamp, 10)
		endOffset := strconv.Itoa(partition.End.Offset)
		// endOffsetTimestamp := strconv.FormatInt(partition.End.Timestamp, 10)

		t.oom.Update(owner+":"+partitionID, partition.End.Offset, time.Now().Unix())

		shouldSendIt, _ := t.tsm.PartitionPut(t.prefix+partitionID, currentLag)
		if !shouldSendIt {
			continue
		}

		topicTag := "topic=" + topic
		partitionTag := "partition=" + partitionID
		ownerTag := "owner=" + owner

		// This part code doesn't work.
		// ie. send a 30s before timestamp doesn't work in wavefront.
		// The goal is to send previous "lag=0" to make metric look better.
		// if shouldSendPreviousLag {
		// 	previousTimestamp, err := strconv.ParseInt(strings.Split(postfix, " ")[0], 10, 64)
		// 	previousTimestamp -= 60
		// 	if err != nil {
		// 		log.Println("ERROR: Cannot parse previousTimestamp in shouldSendPreviousLag.")
		// 		return
		// 	}
		// 	produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{"0", strconv.FormatInt(previousTimestamp, 10), postfix, topicTag, partitionTag, ownerTag})
		// }

		t.ProduceQueue <- combineInfo([]string{t.prefix, topic, partitionID, "Lag"}, []string{strconv.Itoa(currentLag), postfix, topicTag, partitionTag, ownerTag})
		t.ProduceQueue <- combineInfo([]string{t.prefix, topic, partitionID, "startOffset"}, []string{startOffset, postfix, topicTag, partitionTag, ownerTag})
		t.ProduceQueue <- combineInfo([]string{t.prefix, topic, partitionID, "endOffset"}, []string{endOffset, postfix, topicTag, partitionTag, ownerTag})
	}
}

func (t *Translator) parseMaxLagInfo(maxLag protocol.MaxLag, postfix string) {
	// tags: owner
	// metrics: partitionID, currentLag, startOffset, endOffset, topic

	owner := maxLag.Owner
	// it happens when info is invalid, skip this info.
	if owner == "" {
		t.Logger.Warn("owner invalid",
			zap.String("prefix", t.prefix),
			zap.Int("maxLagPartitionID", maxLag.Partition),
			zap.Int64("timestamp", time.Now().Unix()),
		)
		t.CountService.Increase("exception.ownerInvalid", t.env)
		return
	}
	ownerTag := "owner=" + owner

	// MaxLagPartition Level handle
	maxLagMap := make(map[string]string)
	maxLagMap["maxLagmaxLagPartitionID"] = strconv.Itoa(maxLag.Partition)
	maxLagMap["maxLagCurrentLag"] = strconv.Itoa(maxLag.CurrentLag)
	maxLagMap["maxLagStartOffset"] = strconv.Itoa(maxLag.Start.Offset)
	maxLagMap["maxLagEndOffset"] = strconv.Itoa(maxLag.End.Offset)
	maxLagMap["maxLagTopic"] = maxLag.Topic

	for key, value := range maxLagMap {
		t.ProduceQueue <- combineInfo([]string{t.prefix, key}, []string{value, postfix, ownerTag})
	}
}

func getEpochTime() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}

func combineInfo(prefix []string, postfix []string) string {
	return strings.Join(prefix, ".") + " " + strings.Join(postfix, " ")
}
