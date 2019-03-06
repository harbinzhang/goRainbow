package utils

import "sync"

// SyncNestedMap is used for goRainbow parent-info mapping
// No need to use RWMutex, because only main thread would read
// and two threads would write.
type SyncNestedMap struct {
	sync.Mutex
	infoMap    map[string]interface{}
	parentLock map[string]*sync.Mutex
}

func (snm *SyncNestedMap) Init() {
	snm.Lock()
	defer snm.Unlock()

	snm.infoMap = make(map[string]interface{})
	snm.parentLock = make(map[string]*sync.Mutex)
}

// SetLock to set a refined lock, on parent-level,
// to avoid blocking, to improve performance
func (snm *SyncNestedMap) SetLock(parent string) bool {
	snm.Lock()
	defer snm.Unlock()

	if _, ok := snm.infoMap[parent]; !ok {
		return false
	}
	snm.parentLock[parent].Lock()
	return true
}

func (snm *SyncNestedMap) ReleaseLock(parent string) bool {
	snm.Lock()
	defer snm.Unlock()

	if _, ok := snm.infoMap[parent]; !ok {
		return false
	}
	snm.parentLock[parent].Unlock()
	return true
}

func (snm *SyncNestedMap) GetChild(parent string, child interface{}) interface{} {

	snm.Lock()
	defer snm.Unlock()
	if _, ok := snm.infoMap[parent]; ok {

	} else {
		snm.infoMap[parent] = child
		snm.parentLock[parent] = &sync.Mutex{}
	}
	return snm.infoMap[parent]
}

// I cannot enable this method now due to data type conflict, I will dive into it.
// func (snm *SyncNestedMap) DeregisterChild(parent string, consumer string) {
// 	// Refined parent-level lock.
// 	snm.SetLock(parent)
// 	defer snm.ReleaseLock(parent)

// 	// May need to add a judge
// 	delete(snm.infoMap[parent].(map[string]interface{}), consumer)
// }
