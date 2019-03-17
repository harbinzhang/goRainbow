package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HarbinZhang/goRainbow/core/protocol"
)

// OwnerOffsetMoveHelper is for statistics of how many records
// handled per partiton per host per minute.
type OwnerOffsetMoveHelper struct {
	syncMap      *SyncNestedMap
	prefix       string
	postfix      string
	produceQueue chan<- string
}

func (oom *OwnerOffsetMoveHelper) Init(produceQueue chan<- string, prefix string, postfix string) {
	oom.syncMap = &SyncNestedMap{}
	oom.syncMap.Init()

	oom.prefix = prefix
	oom.postfix = postfix
	oom.produceQueue = produceQueue

	go func() {
		ticker := time.NewTicker(60 * time.Second)
		for {
			<-ticker.C
			oom.generateMetrics()
		}
	}()
}

func (oom *OwnerOffsetMoveHelper) Update(key string, offset int, timestamp int64) {
	// It works for the current case. ie. concurrent competition only exists in different key.
	// It doesn't work for "concurrent competition exists in the same key at the same time".
	partitionOffsetMove := oom.GetSyncMap().GetChild(key, protocol.PartitionOffsetMove{}).(protocol.PartitionOffsetMove)

	partitionOffsetMove.LastOffset = partitionOffsetMove.CurtOffset
	partitionOffsetMove.LastTimestamp = partitionOffsetMove.CurtTimestamp

	partitionOffsetMove.CurtOffset = offset
	partitionOffsetMove.CurtTimestamp = timestamp

	oom.GetSyncMap().PutChild(key, partitionOffsetMove)
}

func (oom *OwnerOffsetMoveHelper) generateMetrics() {
	keys := oom.syncMap.GetKeys()

	for _, k := range keys {

		ks := strings.Split(k, ":")
		if len(ks) != 2 {
			// the params are not "host:port" format, skip this one.
			continue
		}

		oom.syncMap.SetLock(k)
		// populate offset move metric
		partitionOffsetMove := oom.syncMap.GetChild(k, protocol.PartitionOffsetMove{}).(protocol.PartitionOffsetMove)
		timeDiff := partitionOffsetMove.CurtTimestamp - partitionOffsetMove.LastTimestamp
		offsetDiff := partitionOffsetMove.CurtOffset - partitionOffsetMove.LastOffset
		ownerTag := "owner=" + ks[0]

		if timeDiff == 30 {
			// it's a risky logic.
			// From my observation, the timeDiff is always stable
			// but it may change when tasks overload
			// just because this way is easy to implement and good for now.
			// I will think of how to get a better solution.
			offsetMove := strconv.Itoa(offsetDiff * 2)
			oom.produceQueue <- combineInfo([]string{oom.prefix, "hosts", ks[1]},
				[]string{offsetMove, strconv.FormatInt(partitionOffsetMove.CurtTimestamp, 10), oom.postfix, ownerTag})
		} else if timeDiff == 60 {
			offsetMove := strconv.Itoa(offsetDiff)
			oom.produceQueue <- combineInfo([]string{oom.prefix, "hosts", ks[1]},
				[]string{offsetMove, strconv.FormatInt(partitionOffsetMove.CurtTimestamp, 10), oom.postfix, ownerTag})
		} else {
			// the precise result should be
			// offsetMove := strconv.FormatInt(int64(float64(offsetDiff*60)/float64(timeDiff)), 10)
			fmt.Println("current time diff is" + strconv.FormatInt(timeDiff, 10))
		}
		oom.syncMap.ReleaseLock(k)
	}
}

func (oom *OwnerOffsetMoveHelper) GetSyncMap() *SyncNestedMap {
	return oom.syncMap
}

func combineInfo(prefix []string, postfix []string) string {
	return strings.Join(prefix, ".") + " " + strings.Join(postfix, " ")
}