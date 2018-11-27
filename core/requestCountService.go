package core

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

// RequestCountService is for count data traffic in go-rainbow
type RequestCountService struct {
	sync.Mutex
	envCount     map[string]int
	interval     time.Duration
	producerChan chan string
	name         string
}

// Init is to initial a RequestCountService
func (rc *RequestCountService) Init() {
	rc.envCount = make(map[string]int)
	go func() {
		ticker := time.NewTicker(rc.interval)
		for {
			<-ticker.C
			rc.generateMetric()
		}
	}()
}

// Increase is for increase message count increase per env
func (rc *RequestCountService) Increase(env string) {
	rc.Lock()
	if value, ok := rc.envCount[env]; ok {
		rc.envCount[env] = value + 1
	} else {
		rc.envCount[env] = 1
	}
	rc.Unlock()
}

// translate all count to metrics and push it to chan
func (rc *RequestCountService) generateMetric() {
	rc.Lock()
	for env, count := range rc.envCount {
		prefix := "fjord.burrow." + env + "." + rc.name
		message := prefix + " " + strconv.Itoa(count)
		fmt.Println("Data traffic produced: " + message)
		rc.producerChan <- message
	}

	rc.envCount = make(map[string]int)
	rc.Unlock()
}
