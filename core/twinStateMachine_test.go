package core

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

func TestPartitionPut(t *testing.T) {
	tsm := &TwinStateMachine{}

	tsm.Init()

	assert.Equal(t, true, tsm.PartitionPut("test", 1), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 1), "Not passed")
	assert.Equal(t, true, tsm.PartitionPut("test", 1), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 1), "Not passed")
	assert.Equal(t, true, tsm.PartitionPut("test", 1), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 1), "Not passed")
	assert.Equal(t, true, tsm.PartitionPut("test", 1), "Not passed")

	assert.Equal(t, true, tsm.PartitionPut("test", 0), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 0), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 0), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 0), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 0), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 0), "Not passed")
	assert.Equal(t, false, tsm.PartitionPut("test", 0), "Not passed")

}
