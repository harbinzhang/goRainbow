package core

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetEpochTime0(t *testing.T) {
	assert.Equal(t, "794109877", getEpochTime("1995-03-02 02:04:37"), "Not passed")
}

func TestGetEpochTime1(t *testing.T) {
	assert.Equal(t, strconv.FormatInt(time.Now().Unix(), 10), getEpochTime("0001-01-01 00:00:00"), "Not passed")
}
