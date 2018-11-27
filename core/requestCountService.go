package core

import (
	"fmt"
	"strconv"
	"strings"
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
	postfix      string
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
	timestamp := getCurrentEpochTime()
	for env, count := range rc.envCount {
		prefix := "fjord.burrow." + env + "." + rc.name
		envTag := "env=" + env
		message := strings.Join([]string{prefix, strconv.Itoa(count), timestamp, rc.postfix, envTag}, " ")
		fmt.Println("Data traffic produced: " + message)
		rc.producerChan <- message
	}

	rc.envCount = make(map[string]int)
	rc.Unlock()
}

func getCurrentEpochTime() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}
