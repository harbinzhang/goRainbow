package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRequestCountServiceSingle(t *testing.T) {
	producerChan := make(chan string, 10)
	rc := &RequestCountService{
		name:         "totalMessage",
		interval:     1 * time.Second,
		producerChan: producerChan,
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
		name:         "totalMessage",
		interval:     100 * time.Millisecond,
		producerChan: producerChan,
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
