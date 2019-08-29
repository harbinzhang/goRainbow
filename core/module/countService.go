package module

import (
	"sync"
	"time"

	"github.com/harbinzhang/goRainbow/core/util"
)

// CountService provide counters which send # of it counts per minute to wavefront.
// Usage:
// counterMap.Init(producerQueue)
// counterMap.Increase(${counterName})
// No need to init each counter, counterService would init it
// at the first time Increase()
type CountService struct {
	sync.RWMutex

	ProduceQueue chan<- string

	counterMap map[string]*util.RequestCounter
	postfix    string
}

// Start is a general start()
func (cc *CountService) Start() {
	cc.counterMap = make(map[string]*util.RequestCounter)

	contextProvider := util.ContextProvider{}
	contextProvider.Init()
	cc.postfix = contextProvider.GetPostfix()
}

// Stop is a general stop()
func (cc *CountService) Stop() error {
	for _, val := range cc.counterMap {
		err := val.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}

// Increase inceases 1 in RequestCounterName of env.
// It would init a new RequestCounter if not exist.
func (cc *CountService) Increase(RequestCounterName string, env string) {
	cc.isExistOrInit(RequestCounterName)
	cc.counterMap[RequestCounterName].Increase(env)
}

// IsCountServiceAvailable is for health_check
func (cc *CountService) IsCountServiceAvailable() bool {
	const TotalMessage string = "totalMessage"
	cc.isExistOrInit(TotalMessage)
	return cc.counterMap[TotalMessage].IsMetricAvailable()
}

// isExistOrInit would init requestCounter if there is no one named RequestCounterName existing.
func (cc *CountService) isExistOrInit(RequestCounterName string) {
	cc.RLock()
	if _, ok := cc.counterMap[RequestCounterName]; !ok {
		cc.RUnlock()
		// It may have a problem. the same counter can be inited multiple times.
		// Here is a race condition.I
		// But it is acceptable.
		cc.Lock()
		// init counter
		rcs := &util.RequestCounter{
			Name:         RequestCounterName,
			Interval:     60 * time.Second,
			ProducerChan: cc.ProduceQueue,
			Postfix:      cc.postfix,
		}
		rcs.Init()
		cc.counterMap[RequestCounterName] = rcs
		cc.Unlock()
	} else {
		cc.RUnlock()
	}
}
