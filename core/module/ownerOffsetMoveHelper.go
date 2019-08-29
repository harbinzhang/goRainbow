package module

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/harbinzhang/goRainbow/core/protocol"
	"github.com/harbinzhang/goRainbow/core/util"
)

// OwnerOffsetMoveHelper is for statistics of how many records
// handled per partiton per host per minute.
type OwnerOffsetMoveHelper struct {
	CountService *CountService
	ProduceQueue chan<- string
	Logger       *zap.Logger

	syncMap     *util.SyncNestedMap
	prefix      string
	postfix     string
	env         string
	tag         string
	ticker      *time.Ticker
	quitChannel chan struct{}
}

// Init is a general Init
func (oom *OwnerOffsetMoveHelper) Init(prefix string, postfix string, env string, tag string) {
	oom.syncMap = &util.SyncNestedMap{}
	oom.syncMap.Init()

	oom.prefix = prefix
	oom.postfix = postfix
	oom.env = env
	oom.tag = tag

	oom.ticker = time.NewTicker(60 * time.Second)
	oom.quitChannel = make(chan struct{})
	go func() {
		for {
			select {
			case <-oom.ticker.C:
				oom.generateMetrics()
			case <-oom.quitChannel:
				return
			}
		}
	}()
}

// Stop is a general stop
func (oom *OwnerOffsetMoveHelper) Stop() error {
	oom.Logger.Info("stopping")

	oom.ticker.Stop()
	close(oom.quitChannel)

	return nil
}

// Update updates current offset for different key.
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
			oom.CountService.Increase("exception.invalidFormat", oom.env)
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
			oom.ProduceQueue <- combineInfo([]string{oom.prefix, oom.tag, ks[1]},
				[]string{offsetMove, strconv.FormatInt(partitionOffsetMove.CurtTimestamp, 10), oom.postfix, ownerTag})
		} else if timeDiff == 60 {
			offsetMove := strconv.Itoa(offsetDiff)
			oom.ProduceQueue <- combineInfo([]string{oom.prefix, oom.tag, ks[1]},
				[]string{offsetMove, strconv.FormatInt(partitionOffsetMove.CurtTimestamp, 10), oom.postfix, ownerTag})
		} else {
			// the precise result should be
			// offsetMove := strconv.FormatInt(int64(float64(offsetDiff*60)/float64(timeDiff)), 10)
			oom.CountService.Increase("exception.timeDiffInvalid", oom.env)
			oom.Logger.Warn("TimeDiff is not valid",
				zap.String("cluster", oom.env),
				zap.String("tag", oom.tag),
				zap.String("prefix", oom.prefix),
				zap.Int64("timeDiff", timeDiff),
				zap.Int64("timestamp", partitionOffsetMove.CurtTimestamp),
			)
			fmt.Println("current time diff is" + strconv.FormatInt(timeDiff, 10))
		}
		oom.syncMap.ReleaseLock(k)
	}
}

// GetSyncMap returns its syncMap
func (oom *OwnerOffsetMoveHelper) GetSyncMap() *util.SyncNestedMap {
	return oom.syncMap
}

func combineInfo(prefix []string, postfix []string) string {
	return strings.Join(prefix, ".") + " " + strings.Join(postfix, " ")
}
