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
func Translator(lagQueue chan config.LagInfo, produceQueue chan string) {

	// Prepare config file
	var conf config.Config
	configFile, _ := os.Open("config/config.json")
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	if err := decoder.Decode(&conf); err != nil {
		fmt.Println("Err conf: ", err)
	}

	// Prepare tags
	dataCenter := "data_center=" + conf.Service.DataCenter
	department := "department=" + conf.Service.Department
	planet := "planet=" + conf.Service.Planet
	serviceName := "service_name=" + conf.Service.Name
	porterTools := "porter_tools=goRainbow"

	// Need to do what
	source := "source=192.168.3.169"
	dcaZone := "dca_zone=local"

	// postfix := "source=192.168.3.169 data_center=slv dca_zone=local department=fjord planet=sbx888 service_name=porter_rainbow porter_tools=porter-rainbow"
	postfix := strings.Join([]string{source, dataCenter, dcaZone, department, planet, serviceName, porterTools}, " ")

	// fmt.Println(postfix)

	// Init RequestCountService for data traffic statistic
	// rcsTotal for total data traffic
	rcsTotal := &RequestCountService{
		name:         "totalMessage",
		interval:     60 * time.Second,
		producerChan: produceQueue,
		postfix:      postfix,
	}
	// rcsValid for valid data traffic(i.e. message with totalLag > 0)
	rcsValid := &RequestCountService{
		name:         "validMessage",
		interval:     60 * time.Second,
		producerChan: produceQueue,
		postfix:      postfix,
	}

	rcsTotal.Init()
	rcsValid.Init()

	for lag := range lagQueue {
		// fmt.Println("trans", lag)
		parseInfo(lag, produceQueue, postfix, rcsTotal, rcsValid)
	}
}

func combineInfo(prefix []string, postfix []string) string {
	return strings.Join(prefix, ".") + " " + strings.Join(postfix, " ")
}

// parseInfo is for LagInfo struct (in config/struct.go),
// which has all partitions info.
func parseInfo(lag config.LagInfo, produceQueue chan string, postfix string, rcsTotal *RequestCountService, rcsValid *RequestCountService) {
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

		rcsTotal.Increase(cluster)

		// skip partitions info if totalLag == 0
		if totalLag == "0" {
			break
		}

		// The current message is valid, with totalLag > 0
		rcsValid.Increase(cluster)

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

			topicPostfix := "topic=" + topic

			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{currentLag, timestamp, newPostfix, topicPostfix})
			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "startOffset"}, []string{startOffset, timestamp, newPostfix, topicPostfix})
			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "endOffset"}, []string{endOffset, timestamp, newPostfix, topicPostfix})
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
