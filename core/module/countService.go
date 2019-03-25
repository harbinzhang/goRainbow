package module

import (
	"time"

	"github.com/HarbinZhang/goRainbow/core/util"
)

type CountService struct {
	countService map[string]*util.RequestCounter
}

func (cc *CountService) Init(produceQueue chan<- string) {
	cc.countService = make(map[string]*util.RequestCounter)

	contextProvider := util.ContextProvider{}
	contextProvider.Init("config/config.json")
	postfix := contextProvider.GetPostfix()

	// Preapre rcs for total metrics traffic.
	// Using it for health_check
	rcsTotal := &util.RequestCounter{
		Name:         "totalMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
		Postfix:      postfix,
	}
	cc.countService["totalMessage"] = rcsTotal

	// rcsValid is for valid data traffic(i.e. message with totalLag > 0)
	rcsValid := &util.RequestCounter{
		Name:         "validMessage",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
		Postfix:      postfix,
	}
	cc.countService["validMessage"] = rcsValid

	// rcsException is for # of exceptions per minute.
	rcsException := &util.RequestCounter{
		Name:         "exceptionCount",
		Interval:     60 * time.Second,
		ProducerChan: produceQueue,
		Postfix:      postfix,
	}
	cc.countService["exceptionCount"] = rcsException

	for _, rcs := range cc.countService {
		rcs.Init()
	}
}

func (cc *CountService) Increase(RequestCounterName string, env string) {
	cc.countService[RequestCounterName].Increase(env)
}

func (cc *CountService) IsCountServiceAvailable() bool {
	return cc.countService["totalMessage"].IsMetricAvailable()
}
