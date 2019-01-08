package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	tsm := &TwinStateMachine{}

	tsm.Init()

	assert.Equal(t, true, tsm.Put("test", 1), "Not passed")
	assert.Equal(t, false, tsm.Put("test", 1), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 1), "Not passed")
	assert.Equal(t, false, tsm.Put("test", 1), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 1), "Not passed")
	assert.Equal(t, false, tsm.Put("test", 1), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 1), "Not passed")

	assert.Equal(t, true, tsm.Put("test", 3), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 2), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 1), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 3), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 4), "Not passed")
	assert.Equal(t, true, tsm.Put("test", 0), "Not passed")

	assert.Equal(t, false, tsm.Put("test", 0), "Not passed")
	assert.Equal(t, false, tsm.Put("test", -1), "Not passed")
	assert.Equal(t, false, tsm.Put("test", -1), "Not passed")

}

func TestPartitionPutBasic(t *testing.T) {
	tsm := &TwinStateMachine{}

	tsm.Init()

	res, _ := tsm.PartitionPut("test", 1)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 1)
	assert.Equal(t, false, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 1)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 1)
	assert.Equal(t, false, res, "Not passed")
}

func TestPartitionPutShouldSendPreviousLag(t *testing.T) {
	tsm := &TwinStateMachine{}

	tsm.Init()

	_, res := tsm.PartitionPut("test", 1)
	assert.Equal(t, false, res, "Not passed")
	_, res = tsm.PartitionPut("test", 0)
	assert.Equal(t, false, res, "Not passed")
	_, res = tsm.PartitionPut("test", 0)
	assert.Equal(t, false, res, "Not passed")
	_, res = tsm.PartitionPut("test", 1)
	assert.Equal(t, true, res, "Not passed")

	_, res = tsm.PartitionPut("test", 0)
	assert.Equal(t, false, res, "Not passed")
	_, res = tsm.PartitionPut("test", 1)
	assert.Equal(t, false, res, "Not passed")

	_, res = tsm.PartitionPut("test", 0)
	assert.Equal(t, false, res, "Not passed")
	_, res = tsm.PartitionPut("test", 0)
	assert.Equal(t, false, res, "Not passed")
	_, res = tsm.PartitionPut("test", 1)
	assert.Equal(t, true, res, "Not passed")
}

func TestPartitionPutShouldSendAllTheTime(t *testing.T) {
	tsm := &TwinStateMachine{}

	tsm.Init()

	res, _ := tsm.PartitionPut("test", 1)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 2)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 1)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 2)
	assert.Equal(t, true, res, "Not passed")

	res, _ = tsm.PartitionPut("test", 3)
	assert.Equal(t, true, res, "Not passed")

	res, _ = tsm.PartitionPut("test", 4)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 5)
	assert.Equal(t, true, res, "Not passed")
}

func TestPartitionPutSkipFirst0(t *testing.T) {
	tsm := &TwinStateMachine{}

	tsm.Init()

	res, _ := tsm.PartitionPut("test", 1)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 2)
	assert.Equal(t, true, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 0)
	assert.Equal(t, false, res, "Not passed")
	res, _ = tsm.PartitionPut("test", 0)
	assert.Equal(t, true, res, "Not passed")
}
