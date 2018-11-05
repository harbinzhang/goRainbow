package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/HarbinZhang/goRainbow/config"
	"github.com/HarbinZhang/goRainbow/core"
)

// TODO: exception handle
// TODO: logging
// TODO:

func main() {
	lagQueue := make(chan config.LagInfo, 3000)
	produceQueue := make(chan string, 3000)
	// go producer.Produce(lagQueue)
	go core.Translator(lagQueue, produceQueue)
	go core.Produce(produceQueue)
	lagHandler := consumeLag(lagQueue)
	http.HandleFunc("/rainbow/lag", lagHandler)
	http.ListenAndServe(":7099", nil)
	fmt.Println("server exited")
}

func consumeLag(lagQueue chan config.LagInfo) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r.Body)
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
