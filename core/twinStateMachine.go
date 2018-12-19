package core

import "sync"

// TwinStateMachine is for cleverly sending metrics to wavefront
// When lag changes observingly, will send metrics per 30s
// When lag doesn't change, or is 0, will send metrics per 60s
//
// So we have logic:
// currentLag    PreviousLag     send?      save
//      0           0            No          -1
//      0           not 0        Yes         0
//      X(not 0)    0            Yes         X
//      X(not 0)    X(not 0)     No          -1
//      X(not 0)    Y(not 0)     Yes         X
// After simplify:
//	CurrentLag == PreviousLag	Not send, save -1
//  CurrentLag != PreviousLag	Send, save CurrentLag
type TwinStateMachine struct {
	sync.Mutex
	mmap map[string]int
}

func (tsm *TwinStateMachine) Init() {
	tsm.mmap = make(map[string]int)
}

// Put if lag changes, send it to wavefront in 30s(true), otherwise 60s(false)
func (tsm *TwinStateMachine) Put(key string, lag int) bool {
	tsm.Lock()
	defer tsm.Unlock()

	if val, ok := tsm.mmap[key]; ok {
		if val == lag {
			tsm.mmap[key] = -1
			return false
		} else {
			tsm.mmap[key] = lag
			return true
		}
	} else {
		tsm.mmap[key] = lag
		return true
	}
}

// PartitionPut is for Partition level use, it's different from general put
// in Partition level, it would keep not sending metrics if lag is 0.
func (tsm *TwinStateMachine) PartitionPut(key string, lag int) bool {
	tsm.Lock()
	defer tsm.Unlock()

	if val, ok := tsm.mmap[key]; ok {
		if lag == 0 && val == 0 {
			return false
		} else if lag == val {
			tsm.mmap[key] = -1
			return false
		} else {
			tsm.mmap[key] = lag
			return true
		}
	} else {
		tsm.mmap[key] = lag
		return true
	}
}
