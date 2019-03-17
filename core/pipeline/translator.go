package pipeline

import (
	"strconv"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/core/modules"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/utils"
)

// Translator for message translate from struct to string
func Translator(lagQueue <-chan protocol.LagStatus, produceQueue chan<- string,
	countService *modules.CountService, prefix string, env string) {

	defer logger.Sync()

	contextProvider := utils.ContextProvider{}
	contextProvider.Init("config/config.json")
	postfix := contextProvider.GetPostfix()

	// Prepare metrics traffic control
	tsm := &utils.TwinStateMachine{}
	tsm.Init()

	// Prepare consumer side offset change per minute
	oom := &modules.OwnerOffsetMoveHelper{CountService: countService}
	oom.Init(produceQueue, prefix, postfix, env)

	for lag := range lagQueue {
		// if lag doesn't change, sends it per 60s. Otherwise 30s.
		shouldSendIt := tsm.Put(lag.Status.Cluster+lag.Status.Group, lag.Status.Totallag)
		if !shouldSendIt {
			continue
		}
		go parseInfo(lag, produceQueue, postfix, countService, tsm, oom)
	}
}

func combineInfo(prefix []string, postfix []string) string {
	return strings.Join(prefix, ".") + " " + strings.Join(postfix, " ")
}

func parseInfo(lag protocol.LagStatus, produceQueue chan<- string, postfix string, countService *modules.CountService,
	tsm *utils.TwinStateMachine, oom *modules.OwnerOffsetMoveHelper) {
	// lag is 0 or non-zero.
	// parse it into lower level(partitions, maxlag).
	cluster := lag.Status.Cluster
	group := lag.Status.Group
	totalLag := strconv.Itoa(lag.Status.Totallag)
	timestamp := getEpochTime()

	envTag := "env=" + cluster
	consumerTag := "consumer=" + group
	newPostfix := strings.Join([]string{timestamp, postfix, envTag, consumerTag}, " ")

	countService.Increase("totalMessage", cluster)

	// prepare prefix = "fjord.burrow.{cluster}.{group}"
	var sb strings.Builder
	sb.WriteString("fjord.burrow.")
	sb.WriteString(cluster + ".")
	sb.WriteString(group)
	prefix := sb.String()

	// fmt.Printf("Handled: %s at %s with totalLag %s\n", group, timestamp, totalLag)
	// log.Printf("Handled: %s at %s with totalLag %s\n", group, timestamp, totalLag)

	produceQueue <- combineInfo([]string{prefix, "totalLag"}, []string{totalLag, newPostfix})

	if totalLag != "0" {
		countService.Increase("validMessage", cluster)
	}

	go parsePartitionInfo(lag.Status.Partitions, produceQueue, prefix, newPostfix, countService, tsm, oom)
	go parseMaxLagInfo(lag.Status.Maxlag, produceQueue, prefix, newPostfix)
}

func parsePartitionInfo(partitions []protocol.Partition, produceQueue chan<- string, prefix string, postfix string,
	countService *modules.CountService, tsm *utils.TwinStateMachine, oom *modules.OwnerOffsetMoveHelper) {
	for _, partition := range partitions {

		owner := partition.Owner
		// it happens when info is invalid, skip this info.
		if owner == "" {
			return
		}

		partitionID := strconv.Itoa(partition.Partition)
		currentLag := partition.CurrentLag

		topic := partition.Topic

		startOffset := strconv.Itoa(partition.Start.Offset)
		// startOffsetTimestamp := strconv.FormatInt(partition.Start.Timestamp, 10)
		endOffset := strconv.Itoa(partition.End.Offset)
		// endOffsetTimestamp := strconv.FormatInt(partition.End.Timestamp, 10)

		oom.Update(owner+":"+partitionID, partition.End.Offset, time.Now().Unix())

		shouldSendIt, _ := tsm.PartitionPut(prefix+partitionID, currentLag)
		if !shouldSendIt {
			continue
		}

		topicTag := "topic=" + topic
		partitionTag := "partition=" + partitionID
		ownerTag := "owner=" + owner

		// This part code doesn't work.
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

		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{strconv.Itoa(currentLag), postfix, topicTag, partitionTag, ownerTag})
		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "startOffset"}, []string{startOffset, postfix, topicTag, partitionTag, ownerTag})
		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "endOffset"}, []string{endOffset, postfix, topicTag, partitionTag, ownerTag})
	}
}

func parseMaxLagInfo(maxLag protocol.MaxLag, produceQueue chan<- string, prefix string, postfix string) {
	// tags: owner
	// metrics: partitionID, currentLag, startOffset, endOffset, topic

	owner := maxLag.Owner
	// it happens when info is invalid, skip this info.
	if owner == "" {
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
		produceQueue <- combineInfo([]string{prefix, key}, []string{value, postfix, ownerTag})
	}
}

func getEpochTime() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}
