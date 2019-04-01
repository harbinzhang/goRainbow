package util

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRequestCountServiceSingle(t *testing.T) {
	producerChan := make(chan string, 10)
	rc := &RequestCounter{
		Name:         "totalMessage",
		Interval:     1 * time.Second,
		ProducerChan: producerChan,
	}
	rc.Init()
	rc.Increase("test")

	time.Sleep(1 * time.Second)
	res := <-producerChan
	assert.Equal(t, "1", strings.Split(res, " ")[1], "message not correct")

	rc.Increase("test")
	rc.Increase("test")

	time.Sleep(1 * time.Second)
	res = <-producerChan
	assert.Equal(t, "2", strings.Split(res, " ")[1], "message not correct")

}

func TestRequestCountServiceMulti(t *testing.T) {
	producerChan := make(chan string, 10)
	rc := &RequestCounter{
		Name:         "totalMessage",
		Interval:     100 * time.Millisecond,
		ProducerChan: producerChan,
	}
	rc.Init()
	rc.Increase("test")
	rc.Increase("test")
	rc.Increase("staging2")
	rc.Increase("staging2")
	time.Sleep(120 * time.Millisecond)
	res := <-producerChan
	assert.Equal(t, "2", strings.Split(res, " ")[1], "message not correct")
	res = <-producerChan
	assert.Equal(t, "2", strings.Split(res, " ")[1], "message not correct")
}

func TestMetricsIsAvailable(t *testing.T) {
	producerChan := make(chan string, 10)
	rc := &RequestCounter{
		Name:         "totalMessage",
		Interval:     10 * time.Millisecond,
		ProducerChan: producerChan,
	}
	rc.Init()
	assert.Equal(t, true, rc.IsMetricAvailable(), "IsMetricAvailable should return true")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, true, rc.IsMetricAvailable(), "IsMetricAvailable should return true")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, false, rc.IsMetricAvailable(), "IsMetricAvailable should return true")
	rc.Increase("test")
	assert.Equal(t, false, rc.IsMetricAvailable(), "IsMetricAvailable should return true")
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, true, rc.IsMetricAvailable(), "IsMetricAvailable should return true")
}
