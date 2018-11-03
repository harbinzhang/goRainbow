package core

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/config"
)

// Translator for message translate from struct to string
func Translator(lagQueue chan config.LagMessage, produceQueue chan string) {

	for lag := range lagQueue {
		// fmt.Println("trans", lag)
		parseMessage(lag, produceQueue)
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
	var maxLagPartitionID string
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
		case "MaxLagPartitionID":
			maxLagPartitionID = field.Value
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
	maxLagBuilder.WriteString(maxLagPartitionID + ".")
	maxLagBuilder.WriteString("maxLag" + " ")
	maxLagBuilder.WriteString(maxLagPartitionLag + " ")
	maxLagBuilder.WriteString(timestamp + " ")
	maxLagBuilder.WriteString(postfix)
	maxLagMessage := maxLagBuilder.String()
	produceQueue <- maxLagMessage

	var startOffsetBuilder strings.Builder
	startOffsetBuilder.WriteString(prefix + maxLagTopic + ".")
	startOffsetBuilder.WriteString(maxLagPartitionID + ".")
	startOffsetBuilder.WriteString("startOffset" + " ")
	startOffsetBuilder.WriteString(startOffset + " ")
	startOffsetBuilder.WriteString(timestamp + " ")
	startOffsetBuilder.WriteString(postfix)
	startOffsetMessage := startOffsetBuilder.String()
	produceQueue <- startOffsetMessage

	var endOffsetBuilder strings.Builder
	endOffsetBuilder.WriteString(prefix + maxLagTopic + ".")
	endOffsetBuilder.WriteString(maxLagPartitionID + ".")
	endOffsetBuilder.WriteString("endOffset" + " ")
	endOffsetBuilder.WriteString(endOffset + " ")
	endOffsetBuilder.WriteString(timestamp + " ")
	endOffsetBuilder.WriteString(postfix)
	endOffsetMessage := endOffsetBuilder.String()
	produceQueue <- endOffsetMessage

}

func getEpochTime(str string) string {
	layout := "2006-01-02T15:04:05Z"
	if str == "0001-01-01T00:00:00Z" {
		return strconv.FormatInt(time.Now().Unix(), 10)
	}
	t, err := time.Parse(layout, str)
	if err != nil {
		fmt.Println("err: ", err)
	}

	return strconv.FormatInt(t.Unix(), 10)
}
