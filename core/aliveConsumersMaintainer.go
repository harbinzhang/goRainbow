package core

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/HarbinZhang/goRainbow/config"
)

// AliveConsumersMaintainer is a maintainer for alive consumers
// It checks Burrow periodically to see if there is a new consumer, then creates a new thread for this consumer.
func AliveConsumersMaintainer(link string, lagStatusQueue chan config.LagStatus) {
	clusterConsumerMap := &SyncNestedMap{}
	clusterConsumerMap.Init()
	for {
		clusters, clusterLink := GetClusters(link)
		if clusters == nil {
			time.Sleep(1 * time.Minute)
			continue
		}
		for _, cluster := range clusters.([]interface{}) {
			clusterString := cluster.(string)
			consumersSet := clusterConsumerMap.GetConsumers(clusterString)

			clusterConsumerMap.SetLock(clusterString)

			consumers, consumersLink := GetConsumers(clusterLink, clusterString)
			fmt.Println(consumers, consumersLink)

			// create new go routine if consumer not exists.
			for _, consumer := range consumers.([]interface{}) {
				consumerString := consumer.(string)
				if _, ok := consumersSet[consumerString]; !ok {
					// A new consumer found, need to 1. create new thread 2. put it into map.
					consumersSet[consumerString] = true
					go NewConsumerForLag(consumersLink, consumerString, clusterString, lagStatusQueue, clusterConsumerMap)
				}
			}

			clusterConsumerMap.ReleaseLock(clusterString)
		}
		time.Sleep(5 * time.Minute)
	}
}

// NewConsumerForLag is a thread to handle new found consumer
func NewConsumerForLag(consumersLink string, consumer string, cluster string, lagStatusQueue chan config.LagStatus, snm *SyncNestedMap) {
	fmt.Println("New consumer found: ", consumersLink, consumer)
	var lagStatus config.LagStatus

	ticker := time.NewTicker(60 * time.Second)
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

	snm.DeregisterConsumer(cluster, consumer)
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
