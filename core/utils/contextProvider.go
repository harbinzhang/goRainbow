package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/HarbinZhang/goRainbow/core/protocol"
)

type ContextProvider struct {
	filename string
}

func (cp *ContextProvider) Init(name string) {
	cp.filename = name
}

func (cp *ContextProvider) GetConf() protocol.Config {
	// Prepare config file
	var conf protocol.Config
	configFile, _ := os.Open(cp.filename)
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	if err := decoder.Decode(&conf); err != nil {
		fmt.Println("Err conf: ", err)
	}
	return conf
}

func (cp *ContextProvider) GetPostfix() string {

	conf := cp.GetConf()

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

	return postfix
}
