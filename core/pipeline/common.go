package pipeline

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/harbinzhang/goRainbow/core/util"
	"go.uber.org/zap"
)

var logger *zap.Logger

// PrepareLogger is Deprecated
func PrepareLogger() {
	logger = util.GetLogger()
}

// GetHTTPStruct put HTTP GET body into target
func getHTTPStruct(link string, target interface{}) {
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(link)
	if err != nil {
		logger.Error(err.Error())
	}

	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(target)
}

// getConsumers gets consumers based on cluster
func getConsumers(link string, cluster string) (interface{}, string) {
	consumersLink := link + cluster + "/consumer/"
	return getHTTPSubSlice(consumersLink, "consumers"), consumersLink
}

// getClusters gets clusters
func getClusters(link string) (interface{}, string) {
	// defer Info.Println("Exit getClusters")
	// Info.Println("Into getClusters")
	return getHTTPSubSlice(link, "clusters"), link + "/"
}

// getHTTPSubSlice is getting json value from link
func getHTTPSubSlice(link string, key string) interface{} {
	resp, err := http.Get(link)
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	decode := json.NewDecoder(resp.Body)

	var s interface{}
	err = decode.Decode(&s)
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	// copy needed string slice to res
	res := s.(map[string]interface{})
	return res[key]
}
