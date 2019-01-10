package pipeline

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/HarbinZhang/goRainbow/core/utils"
)

// func TestGetEpochTime0(t *testing.T) {
// 	assert.Equal(t, "794109877", getEpochTime("1995-03-02 02:04:37"), "Not passed")
// }

// func TestGetEpochTime1(t *testing.T) {
// 	assert.Equal(t, strconv.FormatInt(time.Now().Unix(), 10), getEpochTime("0001-01-01 00:00:00"), "Not passed")
// }

func preparePipeline() (chan<- protocol.LagStatus, <-chan string) {
	// prepare
	lagStatusQueue := make(chan protocol.LagStatus, 1000)
	produceQueue := make(chan string, 9000)

	rcsTotal := &utils.RequestCountService{
		Name:         "totalMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
	}
	rcsTotal.Init()

	go Translator(lagStatusQueue, produceQueue, rcsTotal)

	return lagStatusQueue, produceQueue
}

func prepareLag() protocol.LagStatus {

	// read lagMessage basic template
	pullFile, _ := os.Open("../../config/pull_content.json")
	defer pullFile.Close()
	pullRead, _ := ioutil.ReadAll(pullFile)
	var pull protocol.LagStatus

	// transfer the lag template from byte to struct/json
	if err := json.Unmarshal(pullRead, &pull); err != nil {
		fmt.Printf("Err: %s\n", err)
		os.Exit(1)
	}

	return pull
}

func TestBasic(t *testing.T) {

	lagStatusQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	lagStatusQueue <- pull

	metric := <-produceQueue

	fmt.Println("metric: ", metric)

	close(lagStatusQueue)
	// close(produceQueue)

}

func BenchmarkBasic(b *testing.B) {

	lagStatusQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	go func() {
		<-produceQueue
	}()

	for i := 0; i < b.N; i++ {
		lagStatusQueue <- pull
	}

	close(lagStatusQueue)
}

func TestMany(t *testing.T) {

	lagStatusQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	go func() {
		<-produceQueue
	}()

	for i := 0; i < 100; i++ {
		lagStatusQueue <- pull
	}

	close(lagStatusQueue)
}
