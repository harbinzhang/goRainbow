package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRequestCountServiceSingle(t *testing.T) {
	producerChan := make(chan string, 10)
	rc := &RequestCountService{
		Name:         "totalMessage",
		Interval:     1 * time.Second,
		ProducerChan: producerChan,
	}
	rc.Init()
	rc.Increase("test")
	// t.Log(rc.envCount["test"])
	time.Sleep(1 * time.Second)
	res := <-producerChan
	assert.Equal(t, "fjord.burrow.test.totalMessage 1", res, "message not correct")

	rc.Increase("test")
	rc.Increase("test")
	// t.Log(rc.envCount["test"])
	time.Sleep(1 * time.Second)
	res = <-producerChan
	assert.Equal(t, "fjord.burrow.test.totalMessage 2", res, "message not correct")

}

func TestRequestCountServiceMulti(t *testing.T) {
	producerChan := make(chan string, 10)
	rc := &RequestCountService{
		Name:         "totalMessage",
		Interval:     100 * time.Millisecond,
		ProducerChan: producerChan,
	}
	rc.Init()
	rc.Increase("test")
	rc.Increase("staging2")
	rc.Increase("staging2")
	time.Sleep(120 * time.Millisecond)
	res := <-producerChan
	assert.Equal(t, "fjord.burrow.test.totalMessage 1", res, "message not correct")
	res = <-producerChan
	assert.Equal(t, "fjord.burrow.staging2.totalMessage 2", res, "message not correct")
}

func TestMetricsIsAvailable(t *testing.T) {
	producerChan := make(chan string, 10)
	rc := &RequestCountService{
		Name:         "totalMessage",
		Interval:     10 * time.Millisecond,
		ProducerChan: producerChan,
	}
	rc.Init()
	// t.Log(rc.envCount["test"])
	assert.Equal(t, true, rc.MetricsIsAvailable(), "MetricsIsAvailable should return true")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, true, rc.MetricsIsAvailable(), "MetricsIsAvailable should return true")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, false, rc.MetricsIsAvailable(), "MetricsIsAvailable should return true")
	rc.Increase("test")
	assert.Equal(t, false, rc.MetricsIsAvailable(), "MetricsIsAvailable should return true")
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, true, rc.MetricsIsAvailable(), "MetricsIsAvailable should return true")
}
