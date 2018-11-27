package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/HarbinZhang/goRainbow/config"
	"github.com/HarbinZhang/goRainbow/core"
)

// TODO: exception handle
// TODO: logging
// TODO: flag see cpu useage.
// TODO: whether produce is blocking.
// TODO: what if produce fails.
// TODO: what if container fails.

func main() {

	const LagQueueSize int = 1000
	const ProduceQueueSize int = 9000

	lagQueue := make(chan config.LagInfo, LagQueueSize)
	produceQueue := make(chan string, ProduceQueueSize)

	go core.Translator(lagQueue, produceQueue)
	go core.Produce(produceQueue)

	// log
	f, err := os.OpenFile("rainbow_log", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	log.SetOutput(f)
	log.Println("Log setup finished.")

	lagHandler := consumeLag(lagQueue)
	http.HandleFunc("/rainbow/lag", lagHandler)
	http.ListenAndServe(":7099", nil)
	fmt.Println("server exited")
}

func consumeLag(lagQueue chan config.LagInfo) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// fmt.Println(r.Body)
		decoder := json.NewDecoder(r.Body)
		var msg config.LagInfo
		err := decoder.Decode(&msg)
		if err != nil {
			panic(err)
		}
		// fmt.Println(msg)
		lagQueue <- msg
	}
}
