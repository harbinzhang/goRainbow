package pipeline

import (
	"strconv"
	"testing"
	"time"

	"github.com/HarbinZhang/goRainbow/core"
	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/stretchr/testify/assert"
)

func TestGetEpochTime0(t *testing.T) {
	assert.Equal(t, "794109877", getEpochTime("1995-03-02 02:04:37"), "Not passed")
}

func TestGetEpochTime1(t *testing.T) {
	assert.Equal(t, strconv.FormatInt(time.Now().Unix(), 10), getEpochTime("0001-01-01 00:00:00"), "Not passed")
}

func TestSendPreviousLag(t *testing.T) {
	// Queue init
	lagStatusQueue := make(chan protocol.LagStatus, LagQueueSize)
	produceQueue := make(chan string, ProduceQueueSize)

	// Preapre rcs for total metrics traffic.
	// Using it for health_check
	rcsTotal := &core.RequestCountService{
		Name:         "totalMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
	}
	rcsTotal.Init()

	Translator(lagStatusQueue, produceQueue, rcsTotal)

	// prepare data
	lagStatus1 := protocol.LagStatus{}
}
