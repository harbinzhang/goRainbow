package core

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/config"
)

// Translator for message translate from struct to string
func Translator(lagQueue chan config.LagInfo, produceQueue chan string) {

	var conf config.Config
	configFile, _ := os.Open("config/config.json")
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	if err := decoder.Decode(&conf); err != nil {
		fmt.Println("Err: ", err)
	}

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

	for lag := range lagQueue {
		// fmt.Println("trans", lag)
		parseInfo(lag, produceQueue, postfix)
	}
}

func combineInfo(prefix []string, postfix []string) string {
	return strings.Join(prefix, ".") + " " + strings.Join(postfix, " ")
}

func parseInfo(lag config.LagInfo, produceQueue chan string, postfix string) {
	// fmt.Println(lag)

	for _, eventItem := range lag.Events {
		event := eventItem.Event
		cluster := event.Cluster
		group := event.Group
		totalLag := event.TotalLag
		timestamp := getEpochTime(event.Start)

		var sb strings.Builder
		sb.WriteString("fjord.burrow.")
		sb.WriteString(cluster + ".")
		sb.WriteString(group)
		prefix := sb.String()

		fmt.Printf("Handled: %s at %s \n", group, timestamp)
		// combined info is like: "fjord.burrow.cluster.group.totalLag $totalLag timestamp postfix"
		produceQueue <- combineInfo([]string{prefix, "totalLag"}, []string{totalLag, timestamp, postfix})

		for _, partition := range event.Partitions {
			topic := partition.Topic
			partitionID := strconv.Itoa(partition.Partition)
			startOffset := strconv.Itoa(partition.Start.Offset)
			endOffset := strconv.Itoa(partition.End.Offset)
			currentLag := strconv.Itoa(partition.CurrentLag)

			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "Lag"}, []string{currentLag, timestamp, postfix})
			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "startOffset"}, []string{startOffset, timestamp, postfix})
			produceQueue <- combineInfo([]string{prefix, topic, partitionID, "endOffset"}, []string{endOffset, timestamp, postfix})
		}
	}
}

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

	// if totalLag != "0" {
	// 	fmt.Println(lag)
	// }

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
	// Skipping Burrow's timestamp because it's not precise.
	return strconv.FormatInt(time.Now().Unix(), 10)

	// layout := "2006-01-02 15:04:05"
	// // layout := "2006-01-02T15:04:05Z07:00"
	// if str == "0001-01-01 00:00:00" {
	// 	return strconv.FormatInt(time.Now().Unix(), 10)
	// }
	// t, err := time.Parse(layout, str)
	// if err != nil {
	// 	fmt.Println("err: ", err)
	// }

	// return strconv.FormatInt(t.Unix(), 10)
}
