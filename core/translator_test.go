package core

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetEpochTime0(t *testing.T) {
	assert.Equal(t, "794109877", getEpochTime("1995-03-02T02:04:37Z"), "Not passed")
}

func TestGetEpochTime1(t *testing.T) {
	assert.Equal(t, strconv.FormatInt(time.Now().Unix(), 10), getEpochTime("0001-01-01T00:00:00Z"), "Not passed")
}
