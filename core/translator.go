package core

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/config"
)

// Translator for message translate from struct to string
func Translator(lagQueue chan config.LagStatus, produceQueue chan string, rcsTotal *RequestCountService) {

	// Prepare config file
	var conf config.Config
	configFile, _ := os.Open("config/config.json")
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	if err := decoder.Decode(&conf); err != nil {
		fmt.Println("Err conf: ", err)
	}

	// Prepare tags
	department := "department=" + conf.Service.Department
	serviceName := "service_name=" + conf.Service.Name
	metricFormat := "metric_format=" + conf.Translator.MetricFormat

	// Prepare tags from env variables
	dataCenter := "data_center=" + os.Getenv("DATACENTER")
	planet := "planet=" + os.Getenv("ENV")

	dcaZone := "dca_zone=local"
	source := "source=fjord-burrow"

	// postfix := "source=192.168.3.169 data_center=slv dca_zone=local department=fjord planet=sbx888 service_name=porter_rainbow porter_tools=porter-rainbow"
	postfix := strings.Join([]string{source, dataCenter, dcaZone, department, planet, serviceName, metricFormat}, " ")

	// Init RequestCountService for data traffic statistic
	rcsTotal.Postfix = postfix
	// rcsValid for valid data traffic(i.e. message with totalLag > 0)
	rcsValid := &RequestCountService{
		Name:         "validMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
		Postfix:      postfix,
	}
	rcsValid.Init()

	// Prepare metrics traffic control
	tsm := &TwinStateMachine{}
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

func parseInfo(lag config.LagStatus, produceQueue chan string, postfix string,
	rcsTotal *RequestCountService, rcsValid *RequestCountService, tsm *TwinStateMachine) {
	// lag is 0 or non-zero.
	// parse it into lower level(partitions, maxlag).
	cluster := lag.Status.Cluster
	group := lag.Status.Group
	totalLag := strconv.Itoa(lag.Status.Totallag)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	envTag := "env=" + cluster
	consumerTag := "consumer=" + group
	newPostfix := strings.Join([]string{timestamp, postfix, envTag, consumerTag}, " ")

	go rcsTotal.Increase(cluster)

	// prepare prefix = "fjord.burrow.{cluster}.{group}"
	var sb strings.Builder
	sb.WriteString("fjord.burrow.")
	sb.WriteString(cluster + ".")
	sb.WriteString(group)
	prefix := sb.String()

	fmt.Printf("Handled: %s at %s with totalLag %s\n", group, timestamp, totalLag)
	log.Printf("Handled: %s at %s with totalLag %s\n", group, timestamp, totalLag)

	produceQueue <- combineInfo([]string{prefix, "totalLag"}, []string{totalLag, newPostfix})

	if totalLag == "0" {
		return
	}

	go rcsValid.Increase(cluster)

	go parsePartitionInfo(lag.Status.Partitions, produceQueue, prefix, newPostfix, tsm)
	go parseMaxLagInfo(lag.Status.Maxlag, produceQueue, prefix, newPostfix)

}

func parsePartitionInfo(partitions []config.Partition, produceQueue chan string, prefix string, postfix string, tsm *TwinStateMachine) {
	for _, partition := range partitions {
		partitionID := strconv.Itoa(partition.Partition)
		currentLag := partition.CurrentLag
		shouldSendIt := tsm.Put(prefix+partitionID, currentLag)
		if !shouldSendIt {
			continue
		}

		topic := partition.Topic

		startOffset := strconv.Itoa(partition.Start.Offset)
		endOffset := strconv.Itoa(partition.End.Offset)
		owner := partition.Owner

		topicTag := "topic=" + topic
		partitionTag := "partition=" + partitionID
		ownerTag := "owner=" + owner

		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{strconv.Itoa(currentLag), postfix, topicTag, partitionTag, ownerTag})
		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "startOffset"}, []string{startOffset, postfix, topicTag, partitionTag, ownerTag})
		produceQueue <- combineInfo([]string{prefix, topic, partitionID, "endOffset"}, []string{endOffset, postfix, topicTag, partitionTag, ownerTag})
	}
}

func parseMaxLagInfo(maxLag config.MaxLag, produceQueue chan string, prefix string, postfix string) {
	// tags: owner
	// metrics: partitionID, currentLag, startOffset, endOffset, topic

	owner := maxLag.Owner
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

// parseInfo is for LagInfo struct (in config/struct.go),
// which has all partitions info.
func parseInfod(lag config.LagInfo, produceQueue chan string, postfix string, rcsTotal *RequestCountService, rcsValid *RequestCountService) {
	// fmt.Println(lag)

	for _, eventItem := range lag.Events {
		event := eventItem.Event
		cluster := event.Cluster
		group := event.Group
		totalLag := event.TotalLag
		timestamp := getEpochTime(event.Start)

		envTag := "env=" + cluster
		consumerTag := "consumer=" + group
		newPostfix := strings.Join([]string{postfix, envTag, consumerTag}, " ")

		// Prepare prefix
		var sb strings.Builder
		sb.WriteString("fjord.burrow.")
		sb.WriteString(cluster + ".")
		sb.WriteString(group)
		prefix := sb.String()

		fmt.Printf("Handled: %s at %s \n", group, timestamp)
		log.Printf("Handled: %s at %s \n", group, timestamp)
		// combined info is like: "fjord.burrow.[cluster].[group].totalLag $[totalLag] [timestamp] [postfix]"
		produceQueue <- combineInfo([]string{prefix, "totalLag"}, []string{totalLag, timestamp, newPostfix})

		go rcsTotal.Increase(cluster)

		// skip partitions info if totalLag == 0
		if totalLag == "0" {
			break
		}

		// The current message is valid, with totalLag > 0
		go rcsValid.Increase(cluster)

		// MaxLagPartition Level handle
		maxLag := make(map[string]string)
		maxLag["maxLagmaxLagPartitionID"] = strconv.Itoa(event.MaxLagPartition.Partition)
		maxLag["maxLagCurrentLag"] = strconv.Itoa(event.MaxLagPartition.CurrentLag)
		maxLag["maxLagStartOffset"] = strconv.Itoa(event.MaxLagPartition.Start.Offset)
		maxLag["maxLagEndOffset"] = strconv.Itoa(event.MaxLagPartition.End.Offset)
		maxLag["maxLagTopic"] = event.MaxLagPartition.Topic

		for key, value := range maxLag {
			produceQueue <- combineInfo([]string{prefix, key}, []string{value, timestamp, newPostfix})
		}

		// Handle all partitions level lag.
		for _, partition := range event.Partitions {
			topic := partition.Topic
			partitionID := strconv.Itoa(partition.Partition)
			startOffset := strconv.Itoa(partition.Start.Offset)
			endOffset := strconv.Itoa(partition.End.Offset)
			currentLag := strconv.Itoa(partition.CurrentLag)
			owner := partition.Owner

			topicTag := "topic=" + topic
			partitionTag := "partition=" + partitionID
			ownerTag := "owner=" + owner

			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{currentLag, timestamp, newPostfix, topicTag, partitionTag, ownerTag})
			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "startOffset"}, []string{startOffset, timestamp, newPostfix, topicTag, partitionTag, ownerTag})
			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "endOffset"}, []string{endOffset, timestamp, newPostfix, topicTag, partitionTag, ownerTag})
		}
	}
}

// parseMessage is for LagMessage struct(in config/struct.go),
// which has MaxLagPartiion Info.
func parseMessage(lag config.LagMessage, produceQueue chan string) {
	var timestamp string
	var env string
	var group string
	var totalLag string
	var maxLagTopic string
	var startOffset string
	var endOffset string
	var maxLagpartitionID string
	var maxLagPartitionLag string
	for _, field := range lag.Attachments[0].Fields {
		switch field.Title {
		case "Group":
			group = field.Value
		case "Start":
			timestamp = getEpochTime(field.Value)
		case "Total Lag":
			totalLag = field.Value
		case "Cluster":
			env = field.Value
		case "MaxLagpartitionID":
			maxLagpartitionID = field.Value
		case "MaxLagPartitionTopic":
			maxLagTopic = field.Value
		case "MaxLagPartitionStartOffset":
			startOffset = field.Value
		case "MaxLagPartitionEndOffset":
			endOffset = field.Value
		case "MaxLagPartitionLag":
			maxLagPartitionLag = field.Value
		default:
			fmt.Println("Something wrong", field.Title)
		}
	}

	var sb strings.Builder
	sb.WriteString("fjord.burrow.")
	sb.WriteString(env + ".")
	sb.WriteString(group + ".")
	prefix := sb.String()
	postfix := "source=192.168.3.169 data_center=slv dca_zone=local department=fjord planet=sbx888 service_name=porter_rainbow porter_tools=porter-rainbow"

	var totalLagBuilder strings.Builder
	totalLagBuilder.WriteString(prefix)
	totalLagBuilder.WriteString("totalLag" + " ")
	totalLagBuilder.WriteString(totalLag + " ")
	totalLagBuilder.WriteString(timestamp + " ")
	totalLagBuilder.WriteString(postfix)
	totalLagMessage := totalLagBuilder.String()
	produceQueue <- totalLagMessage

	var maxLagBuilder strings.Builder
	maxLagBuilder.WriteString(prefix + maxLagTopic + ".")
	maxLagBuilder.WriteString(maxLagpartitionID + ".")
	maxLagBuilder.WriteString("maxLag" + " ")
	maxLagBuilder.WriteString(maxLagPartitionLag + " ")
	maxLagBuilder.WriteString(timestamp + " ")
	maxLagBuilder.WriteString(postfix)
	maxLagMessage := maxLagBuilder.String()
	produceQueue <- maxLagMessage

	var startOffsetBuilder strings.Builder
	startOffsetBuilder.WriteString(prefix + maxLagTopic + ".")
	startOffsetBuilder.WriteString(maxLagpartitionID + ".")
	startOffsetBuilder.WriteString("startOffset" + " ")
	startOffsetBuilder.WriteString(startOffset + " ")
	startOffsetBuilder.WriteString(timestamp + " ")
	startOffsetBuilder.WriteString(postfix)
	startOffsetMessage := startOffsetBuilder.String()
	produceQueue <- startOffsetMessage

	var endOffsetBuilder strings.Builder
	endOffsetBuilder.WriteString(prefix + maxLagTopic + ".")
	endOffsetBuilder.WriteString(maxLagpartitionID + ".")
	endOffsetBuilder.WriteString("endOffset" + " ")
	endOffsetBuilder.WriteString(endOffset + " ")
	endOffsetBuilder.WriteString(timestamp + " ")
	endOffsetBuilder.WriteString(postfix)
	endOffsetMessage := endOffsetBuilder.String()
	produceQueue <- endOffsetMessage

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
