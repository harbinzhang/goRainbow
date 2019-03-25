package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Subject string
}

func TestBasic(t *testing.T) {
	SyncNestedMap := &SyncNestedMap{}
	SyncNestedMap.Init()

	child := SyncNestedMap.GetChild("parent", make(map[string]bool)).(map[string]bool)
	fmt.Println(child)

	child["ok"] = true

	expected := child["ok"]

	child2 := SyncNestedMap.GetChild("parent", make(map[string]bool)).(map[string]bool)

	assert.Equal(t, expected, child2["ok"], "not passed")

	SyncNestedMap.SetLock("parent")
	delete(child, "ok")
	SyncNestedMap.ReleaseLock("parent")

	child2 = SyncNestedMap.GetChild("parent", make(map[string]bool)).(map[string]bool)

	assert.Equal(t, 0, len(child2), "not passed")
}

func TestStruct(t *testing.T) {
	SyncNestedMap := &SyncNestedMap{}
	SyncNestedMap.Init()

	child := SyncNestedMap.GetChild("parent", make(map[string]testStruct)).(map[string]testStruct)
	fmt.Println(child)

	child["ok"] = testStruct{Subject: "hi"}

	expected := child["ok"]

	child2 := SyncNestedMap.GetChild("parent", make(map[string]testStruct)).(map[string]testStruct)

	assert.Equal(t, expected, child2["ok"], "not passed")

	SyncNestedMap.SetLock("parent")
	delete(child, "ok")
	SyncNestedMap.ReleaseLock("parent")

	child2 = SyncNestedMap.GetChild("parent", make(map[string]testStruct)).(map[string]testStruct)

	assert.Equal(t, 0, len(child2), "not passed")
}
