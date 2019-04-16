package module

import (
	"os"
	"testing"

	"go.uber.org/zap"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	os.Setenv("configPath", "../../config/config.json")
}

func TestUpdate(t *testing.T) {
	produceQueue := make(chan string, 9000)
	countService := &CountService{ProduceQueue: produceQueue}
	countService.Start()

	oom := &OwnerOffsetMoveHelper{
		CountService: countService,
		ProduceQueue: produceQueue,
		Logger:       zap.NewNop(),
	}
	oom.Init("prefix", "postfix", "env", "tag")

	oom.Update("test1:1", 10, 30)
	keys := oom.GetSyncMap().GetKeys()
	assert.Equal(t, 1, len(keys), "keys length should be 1")

	partitionOffsetMove := oom.GetSyncMap().GetChild("test1:1", nil).(protocol.PartitionOffsetMove)
	assert.Equal(t, 10, partitionOffsetMove.CurtOffset, "curt offset should be 10")

	oom.Update("test2:2", 20, 50)
	partitionOffsetMove = oom.GetSyncMap().GetChild("test2:2", nil).(protocol.PartitionOffsetMove)
	assert.Equal(t, 20, partitionOffsetMove.CurtOffset, "curt offset should be 20")

	oom.Update("test1:1", 30, 60)
	partitionOffsetMove = oom.GetSyncMap().GetChild("test1:1", nil).(protocol.PartitionOffsetMove)
	assert.Equal(t, 30, partitionOffsetMove.CurtOffset, "curt offset should be 30")

	// oom.generateMetrics()
	// res1 := <-produceQueue
	// res2 := <-produceQueue

	// assert.Equal(t, "prefix.hosts.1 20 60 postfix owner=test1", res1, "wrong")
	// assert.Equal(t, "prefix.hosts.2 20 50 postfix owner=test2", res2, "wrong")

	close(produceQueue)
}

func TestGenerateMetrics(t *testing.T) {
	produceQueue := make(chan string, 9000)
	countService := &CountService{ProduceQueue: produceQueue}
	countService.Start()

	oom := &OwnerOffsetMoveHelper{
		CountService: countService,
		ProduceQueue: produceQueue,
		Logger:       zap.NewNop(),
	}
	oom.Init("prefix", "postfix", "env", "tag")

	keys := oom.GetSyncMap().GetKeys()
	assert.Equal(t, 0, len(keys), "keys length should be 0")

	oom.GetSyncMap().GetChild("test1:1", protocol.PartitionOffsetMove{4, 3, 2, 1})
	oom.GetSyncMap().GetChild("testInvalid:2", protocol.PartitionOffsetMove{})
	oom.GetSyncMap().GetChild("test2:2", protocol.PartitionOffsetMove{60, 100, 30, 50})

	keys = oom.GetSyncMap().GetKeys()
	assert.Equal(t, 3, len(keys), "keys length should be 3")

	// oom.generateMetrics()
	// res1 := <-produceQueue
	// res2 := <-produceQueue

	// assert.Equal(t, "prefix.hosts.1 2 4 postfix owner=test1", res1, "wrong")
	// assert.Equal(t, "prefix.hosts.2 50 60 postfix owner=test2", res2, "wrong")

	close(produceQueue)
}
