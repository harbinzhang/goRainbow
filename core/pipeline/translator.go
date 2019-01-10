package pipeline

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/utils"
)

// Translator for message translate from struct to string
func Translator(lagQueue chan protocol.LagStatus, produceQueue chan string, rcsTotal *utils.RequestCountService) {

	contextProvider := utils.ContextProvider{}
	contextProvider.Init("config/config.json")
	postfix := contextProvider.GetPostfix()

	// Init RequestCountService for data traffic statistic
	rcsTotal.Postfix = postfix
	// rcsValid for valid data traffic(i.e. message with totalLag > 0)
	rcsValid := &utils.RequestCountService{
		Name:         "validMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
		Postfix:      postfix,
	}
	rcsValid.Init()

	// Prepare metrics traffic control
	tsm := &utils.TwinStateMachine{}
	tsm.Init()

	for lag := range lagQueue {
		// if lag doesn't change, sends it per 60s. Otherwise 30s.
		shouldSendIt := tsm.Put(lag.Status.Cluster+lag.Status.Group, lag.Status.Totallag)
		if !shouldSendIt {
			continue
		}

		go parseInfo(lag, produceQueue, postfix, rcsTotal, rcsValid, tsm)
	}
}

func combineInfo(prefix []string, postfix []string) string {
	return strings.Join(prefix, ".") + " " + strings.Join(postfix, " ")
}

func parseInfo(lag protocol.LagStatus, produceQueue chan string, postfix string,
	rcsTotal *utils.RequestCountService, rcsValid *utils.RequestCountService, tsm *utils.TwinStateMachine) {
	// lag is 0 or non-zero.
	// parse it into lower level(partitions, maxlag).
	cluster := lag.Status.Cluster
	group := lag.Status.Group
	totalLag := strconv.Itoa(lag.Status.Totallag)

	envTag := "env=" + cluster
	consumerTag := "consumer=" + group
	newPostfix := strings.Join([]string{postfix, envTag, consumerTag}, " ")

	go rcsTotal.Increase(cluster)

	// prepare prefix = "fjord.burrow.{cluster}.{group}"
	var sb strings.Builder
	sb.WriteString("fjord.burrow.")
	sb.WriteString(cluster + ".")
	sb.WriteString(group)
	prefix := sb.String()

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	fmt.Printf("Handled: %s at %s with totalLag %s\n", group, timestamp, totalLag)
	log.Printf("Handled: %s at %s with totalLag %s\n", group, timestamp, totalLag)

	produceQueue <- combineInfo([]string{prefix, "totalLag"}, []string{totalLag, newPostfix})

	if totalLag != "0" {
		go rcsValid.Increase(cluster)
	}

	go parsePartitionInfo(lag.Status.Partitions, produceQueue, prefix, newPostfix, tsm)
	go parseMaxLagInfo(lag.Status.Maxlag, produceQueue, prefix, newPostfix)
}

func parsePartitionInfo(partitions []protocol.Partition, produceQueue chan string, prefix string, postfix string, tsm *utils.TwinStateMachine) {
	for _, partition := range partitions {
		partitionID := strconv.Itoa(partition.Partition)
		currentLag := partition.CurrentLag
		shouldSendIt, _ := tsm.PartitionPut(prefix+partitionID, currentLag)
		if !shouldSendIt {
			continue
		}

		topic := partition.Topic

		startOffset := strconv.Itoa(partition.Start.Offset)
		startOffsetTimestamp := strconv.FormatInt(partition.Start.Timestamp, 10)
		endOffset := strconv.Itoa(partition.End.Offset)
		endOffsetTimestamp := strconv.FormatInt(partition.End.Timestamp, 10)
		owner := partition.Owner

		topicTag := "topic=" + topic
		partitionTag := "partition=" + partitionID
		ownerTag := "owner=" + owner

		// if shouldSendPreviousLag {
		// 	previousTimestamp, err := strconv.ParseInt(strings.Split(postfix, " ")[0], 10, 64)
		// 	previousTimestamp -= 60
		// 	if err != nil {
		// 		log.Println("ERROR: Cannot parse previousTimestamp in shouldSendPreviousLag.")
		// 		return
		// 	}
		// 	produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{"0", strconv.FormatInt(previousTimestamp, 10), postfix, topicTag, partitionTag, ownerTag})
		// }

		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{strconv.Itoa(currentLag), endOffsetTimestamp, postfix, topicTag, partitionTag, ownerTag})
		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "startOffset"}, []string{startOffset, startOffsetTimestamp, postfix, topicTag, partitionTag, ownerTag})
		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "endOffset"}, []string{endOffset, endOffsetTimestamp, postfix, topicTag, partitionTag, ownerTag})
	}
}

func parseMaxLagInfo(maxLag protocol.MaxLag, produceQueue chan string, prefix string, postfix string) {
	// tags: owner
	// metrics: partitionID, currentLag, startOffset, endOffset, topic

	owner := maxLag.Owner
	ownerTag := "owner=" + owner

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	// MaxLagPartition Level handle
	maxLagMap := make(map[string]string)
	maxLagMap["maxLagmaxLagPartitionID"] = strconv.Itoa(maxLag.Partition)
	maxLagMap["maxLagCurrentLag"] = strconv.Itoa(maxLag.CurrentLag)
	maxLagMap["maxLagStartOffset"] = strconv.Itoa(maxLag.Start.Offset)
	maxLagMap["maxLagEndOffset"] = strconv.Itoa(maxLag.End.Offset)
	maxLagMap["maxLagTopic"] = maxLag.Topic

	for key, value := range maxLagMap {
		produceQueue <- combineInfo([]string{prefix, key}, []string{value, timestamp, postfix, ownerTag})
	}

}

func getEpochTime(str string) string {
	// Skipping Burrow's timestamp because it's not precise now.
	// I think it's because cluster not stable
	return strconv.FormatInt(time.Now().Unix(), 10)

	// layout := "2006-01-02 15:04:05"
	// // layout := "2006-01-02T15:04:05Z07:00"
	// if str == "0001-01-01 00:00:00" {
	//	// Burrow info level would provide this date, need to verify.
	// 	return strconv.FormatInt(time.Now().Unix(), 10)
	// }
	// t, err := time.Parse(layout, str)
	// if err != nil {
	// 	fmt.Println("err: ", err)
	// }

	// return strconv.FormatInt(t.Unix(), 10)
}
