package utils

import (
	"testing"

	"github.com/HarbinZhang/goRainbow/core/protocol"
	"github.com/stretchr/testify/assert"
)

func TestOwnerOffsetMoveHelper_generateMetrics(t *testing.T) {
	type fields struct {
		syncMap      *SyncNestedMap
		prefix       string
		postfix      string
		produceQueue chan<- string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oom := &OwnerOffsetMoveHelper{
				syncMap:      tt.fields.syncMap,
				prefix:       tt.fields.prefix,
				postfix:      tt.fields.postfix,
				produceQueue: tt.fields.produceQueue,
			}
			oom.generateMetrics()
		})
	}
}

func TestUpdate(t *testing.T) {
	produceQueue := make(chan string, 9000)

	oom := &OwnerOffsetMoveHelper{}
	oom.Init(produceQueue, "prefix", "postfix")

	oom.Update("test1", 10, 30)
	keys := oom.GetSyncMap().GetKeys()
	assert.Equal(t, 1, len(keys), "keys length should be 1")

	partitionOffsetMove := oom.GetSyncMap().GetChild("test1", nil).(protocol.PartitionOffsetMove)
	assert.Equal(t, 10, partitionOffsetMove.CurtOffset, "curt offset should be 10")

	oom.Update("test2", 20, 50)
	partitionOffsetMove = oom.GetSyncMap().GetChild("test2", nil).(protocol.PartitionOffsetMove)
	assert.Equal(t, 20, partitionOffsetMove.CurtOffset, "curt offset should be 20")

	oom.Update("test1", 30, 60)
	partitionOffsetMove = oom.GetSyncMap().GetChild("test1", nil).(protocol.PartitionOffsetMove)
	assert.Equal(t, 30, partitionOffsetMove.CurtOffset, "curt offset should be 30")

	oom.generateMetrics()
	res1 := <-produceQueue
	res2 := <-produceQueue

	assert.Equal(t, "prefix.test1 40 60 postfix", res1, "wrong")
	assert.Equal(t, "prefix.test2 24 50 postfix", res2, "wrong")

	close(produceQueue)
}

func TestGenerateMetrics(t *testing.T) {
	produceQueue := make(chan string, 9000)

	oom := &OwnerOffsetMoveHelper{}
	oom.Init(produceQueue, "prefix", "postfix")

	keys := oom.GetSyncMap().GetKeys()
	assert.Equal(t, 0, len(keys), "keys length should be 0")

	oom.GetSyncMap().GetChild("test1", protocol.PartitionOffsetMove{4, 3, 2, 1})
	oom.GetSyncMap().GetChild("testInvalid", protocol.PartitionOffsetMove{})
	oom.GetSyncMap().GetChild("test2", protocol.PartitionOffsetMove{60, 100, 30, 50})

	keys = oom.GetSyncMap().GetKeys()
	assert.Equal(t, 3, len(keys), "keys length should be 3")

	oom.generateMetrics()
	res1 := <-produceQueue
	res2 := <-produceQueue

	assert.Equal(t, "prefix.test1 60 4 postfix", res1, "wrong")
	assert.Equal(t, "prefix.test2 100 60 postfix", res2, "wrong")

	close(produceQueue)
}
