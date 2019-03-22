package util

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
//
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

// PartitionPut is for Partition level use, it's different from Put:
// 1. It would keep not sending metrics if lag is 0.
// 2. It should send the previous lag(if==0) if current lag != 0 to make sure
// in Partition level, it would keep not sending metrics if lag is 0.
// 3. No need to adjust traffic rate, because already controlled in Put(totalLag).
//
// Using >0, -1, -2 to store 3 states in twoStateMachine.
// @params
// return (shouldSendCurrentLag, shouldSendPreviousLag)
func (tsm *TwinStateMachine) PartitionPut(key string, lag int) (bool, bool) {
	tsm.Lock()
	defer tsm.Unlock()

	shouldSendCurrentLag := false
	shouldSendPreviousLag := false
	if val, ok := tsm.mmap[key]; ok {
		if lag == 0 && val == 0 {
			// skip this lag
		} else if lag == 0 && val > 0 {
			tsm.mmap[key] = -2
		} else {
			tsm.mmap[key] = lag
			shouldSendCurrentLag = true
		}
		if lag > 0 && val == 0 {
			shouldSendPreviousLag = true
		}
	} else {
		tsm.mmap[key] = lag
		shouldSendCurrentLag = true
	}

	return shouldSendCurrentLag, shouldSendPreviousLag
}
