package pipeline

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/harbinzhang/goRainbow/core/module"
	"github.com/harbinzhang/goRainbow/core/protocol"
	"github.com/harbinzhang/goRainbow/core/util"
)

func TestMain(m *testing.M) {
	PrepareLogger()
	os.Setenv("configPath", "../../config/config.json")
}

func TestWithProducer(t *testing.T) {

	contextProvider := util.ContextProvider{}
	contextProvider.Init()

	lagInfoQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	go func() {
		<-produceQueue
	}()

	for i := 0; i < 100; i++ {
		lagInfoQueue <- pull
		time.Sleep(30 * time.Second)
	}

	select {}

}

func TestStartFrom0(t *testing.T) {

	contextProvider := util.ContextProvider{}
	contextProvider.Init()

	lagInfoQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	go func() {
		<-produceQueue
	}()

	pull.Lag.Status.Partitions[0].CurrentLag = 0
	for i := 0; i < 5; i++ {
		lagInfoQueue <- pull
		time.Sleep(30 * time.Second)
	}

	pull.Lag.Status.Partitions[0].CurrentLag = 50
	for i := 0; i < 5; i++ {
		lagInfoQueue <- pull
		time.Sleep(30 * time.Second)
	}

	select {}
}

func TestBasic(t *testing.T) {

	lagInfoQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	lagInfoQueue <- pull

	metric := <-produceQueue

	fmt.Println("metric: ", metric)

	close(lagInfoQueue)
	// close(produceQueue)

}

func BenchmarkBasic(b *testing.B) {
	b.ReportAllocs()

	lagInfoQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	go func() {
		<-produceQueue
	}()

	for i := 0; i < b.N; i++ {
		lagInfoQueue <- pull
	}

	close(lagInfoQueue)
}

func Benchmark100Consumers(b *testing.B) {
	b.ReportAllocs()

	lagInfoQueue, produceQueue := preparePipeline()
	pull := prepareLag100Consumers()

	b.ResetTimer()

	go func() {
		<-produceQueue
	}()

	for i := 0; i < b.N; i++ {
		lagInfoQueue <- pull[i%100]
	}

	close(lagInfoQueue)
}

func TestMany(t *testing.T) {

	lagInfoQueue, produceQueue := preparePipeline()
	pull := prepareLag()

	go func() {
		<-produceQueue
	}()

	for i := 0; i < 100; i++ {
		lagInfoQueue <- pull
	}

	close(lagInfoQueue)
}

// func TestGetEpochTime0(t *testing.T) {
// 	assert.Equal(t, "794109877", getEpochTime("1995-03-02 02:04:37"), "Not passed")
// }

// func TestGetEpochTime1(t *testing.T) {
// 	assert.Equal(t, strconv.FormatInt(time.Now().Unix(), 10), getEpochTime("0001-01-01 00:00:00"), "Not passed")
// }

func preparePipeline() (chan<- protocol.LagInfo, chan string) {
	// prepare
	lagInfoQueue := make(chan protocol.LagInfo, 1000)
	produceQueue := make(chan string, 9000)

	countService := &module.CountService{
		ProduceQueue: produceQueue,
	}

	translator := &Translator{
		LagQueue:     lagInfoQueue,
		ProduceQueue: produceQueue,
		CountService: countService,
		Logger: util.GetLogger().With(
			zap.String("module", "Translator"),
		),
	}

	translator.Init("prefix", "test")
	go translator.Start()

	return lagInfoQueue, produceQueue
}

func prepareLag() protocol.LagInfo {

	// read lagMessage basic template
	pullFile, _ := os.Open("../../config/pull_content.json")
	defer pullFile.Close()
	pullRead, _ := ioutil.ReadAll(pullFile)
	var pull protocol.LagInfo

	// transfer the lag template from byte to struct/json
	if err := json.Unmarshal(pullRead, &pull.Lag); err != nil {
		fmt.Printf("Err: %s\n", err)
		os.Exit(1)
	}
	pull.Timestamp = time.Now().Unix()

	return pull
}

func prepareLag100Consumers() [100]protocol.LagInfo {
	lag := prepareLag()
	var res [100]protocol.LagInfo

	for i := 0; i < 100; i++ {
		newLag := lag
		copy(newLag.Lag.Status.Partitions, lag.Lag.Status.Partitions)
		Group := "console-consumer-" + strconv.Itoa(i)
		newLag.Lag.Status.Group = Group
		newLag.Lag.Status.Totallag = rand.Intn(10000)
		newLag.Lag.Status.Cluster = lag.Lag.Status.Cluster
		newLag.Lag.Status.Maxlag = lag.Lag.Status.Maxlag
		res[i] = newLag
	}

	return res
}
