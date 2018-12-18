package core

import "sync"

// SyncNestedMap is used for goRainbow cluster-info mapping
// No need to use RWMutex, because only main thread would read
// and two threads would write.
type SyncNestedMap struct {
	sync.Mutex
	infoMap     map[string]map[string]bool
	clusterLock map[string]*sync.Mutex
}

func (snm *SyncNestedMap) Init() {
	snm.Lock()
	defer snm.Unlock()

	snm.infoMap = make(map[string]map[string]bool)
	snm.clusterLock = make(map[string]*sync.Mutex)
}

func (snm *SyncNestedMap) SetLock(cluster string) bool {
	snm.Lock()
	defer snm.Unlock()

	if _, ok := snm.infoMap[cluster]; !ok {
		return false
	}
	snm.clusterLock[cluster].Lock()
	return true
}

func (snm *SyncNestedMap) ReleaseLock(cluster string) bool {
	snm.Lock()
	defer snm.Unlock()

	if _, ok := snm.infoMap[cluster]; !ok {
		return false
	}
	snm.clusterLock[cluster].Unlock()
	return true
}

func (snm *SyncNestedMap) GetConsumers(cluster string) map[string]bool {
	snm.Lock()
	defer snm.Unlock()
	if _, ok := snm.infoMap[cluster]; ok {

	} else {
		snm.infoMap[cluster] = make(map[string]bool)
		snm.clusterLock[cluster] = &sync.Mutex{}
	}
	return snm.infoMap[cluster]
}

func (snm *SyncNestedMap) DeregisterConsumer(cluster string, consumer string) {
	snm.Lock()
	defer snm.Unlock()

	delete(snm.infoMap[cluster], consumer)
}
