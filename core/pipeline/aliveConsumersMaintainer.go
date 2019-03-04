package pipeline

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/utils"
)

// AliveConsumersMaintainer is a maintainer for alive consumers
// It checks Burrow periodically to see if there is a new consumer, then creates a new thread for this consumer.
func AliveConsumersMaintainer(link string, produceQueue chan string, rcsTotal *utils.RequestCountService) {
	clusterConsumerMap := &utils.SyncNestedMap{}
	clusterConsumerMap.Init()

	contextProvider := utils.ContextProvider{}
	contextProvider.Init("config/config.json")
	blacklist := contextProvider.GetBlacklist()

	// rcsValid for valid data traffic(i.e. message with totalLag > 0)
	rcsValid := &utils.RequestCountService{
		Name:         "validMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
	}
	rcsValid.Init()

	for {
		clusters, clusterLink := GetClusters(link)
		if clusters == nil {
			// Burrow server is not ready
			time.Sleep(1 * time.Minute)
			continue
		}
		for _, cluster := range clusters.([]interface{}) {
			clusterString := cluster.(string)
			consumersSet := clusterConsumerMap.GetChild(clusterString)

			clusterConsumerMap.SetLock(clusterString)

			consumers, consumersLink := GetConsumers(clusterLink, clusterString)
			fmt.Println(consumers, consumersLink)

			// create new go routine if consumer not exists.
			for _, consumer := range consumers.([]interface{}) {
				consumerString := consumer.(string)
				if _, ok := consumersSet[consumerString]; !ok {
					// A new consumer found, need to: 1. create new thread 2. put it into map.
					consumersSet[consumerString] = true
					if isInBlacklist, _ := regexp.MatchString(blacklist, consumerString); isInBlacklist {
						// if consumer name is in blacklist, put it in map and
						// skip initiating its consumer handler.
						continue
					}
					go NewConsumerForLag(consumersLink, consumerString, clusterString, clusterConsumerMap, produceQueue, rcsTotal, rcsValid)
				}
			}

			clusterConsumerMap.ReleaseLock(clusterString)
		}
		time.Sleep(5 * time.Minute)
	}
}

// NewConsumerForLag is a thread to handle new found consumer
func NewConsumerForLag(consumersLink string, consumer string, cluster string, snm *utils.SyncNestedMap,
	produceQueue chan string, rcsTotal *utils.RequestCountService, rcsValid *utils.RequestCountService) {
	fmt.Println("New consumer found: ", consumersLink, consumer)
	var lagStatus protocol.LagStatus

	lagStatusQueue := make(chan protocol.LagStatus)

	ticker := time.NewTicker(30 * time.Second)

	go Translator(lagStatusQueue, produceQueue, rcsTotal, rcsValid)

	for {
		// check its consumer lag from Burrow periodically
		<-ticker.C
		HTTPGetStruct(consumersLink+consumer+"/lag", &lagStatus)
		if lagStatus.Error {
			break
		}
		// fmt.Println(lagStatus)
		lagStatusQueue <- lagStatus
	}

	snm.DeregisterChild(cluster, consumer)

	close(lagStatusQueue)
	log.Fatalf("Consumer is invalid: %s\tcluster:%s\n", consumer, cluster)
}

// GetConsumers gets consumers based on cluster
func GetConsumers(link string, cluster string) (interface{}, string) {
	consumersLink := link + cluster + "/consumer/"
	return HTTPGetSubSlice(consumersLink, "consumers"), consumersLink
}

// GetClusters gets clusters
func GetClusters(link string) (interface{}, string) {
	// defer Info.Println("Exit getClusters")
	// Info.Println("Into getClusters")
	return HTTPGetSubSlice(link, "clusters"), link + "/"
}

// HTTPGetStruct put HTTP GET body into target
func HTTPGetStruct(link string, target interface{}) {
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(link)
	if err != nil {
		log.Println(err)
	}

	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(target)
}

// HTTPGetSubSlice is getting json value from link
func HTTPGetSubSlice(link string, key string) interface{} {
	resp, err := http.Get(link)
	if err != nil {
		log.Println(err)
		return nil
	}

	decode := json.NewDecoder(resp.Body)

	var s interface{}
	err = decode.Decode(&s)
	if err != nil {
		log.Println(err)
		return nil
	}

	// copy needed string slice to res
	res := s.(map[string]interface{})
	return res[key]
}
