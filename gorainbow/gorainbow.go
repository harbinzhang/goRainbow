package main

import (
	"encoding/json"
	"net/http"

	"github.com/HarbinZhang/goRainbow/config"
	"github.com/HarbinZhang/goRainbow/core"
)

func main() {
	lagQueue := make(chan config.LagMessage, 3000)
	produceQueue := make(chan string, 3000)
	// go producer.Produce(lagQueue)
	go core.Translator(lagQueue, produceQueue)
	go core.Produce(produceQueue)
	lagHandler := consumeLag(lagQueue)
	http.HandleFunc("/rainbow/lag", lagHandler)
	http.ListenAndServe(":7099", nil)
}

func consumeLag(lagQueue chan config.LagMessage) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var msg config.LagMessage
		err := decoder.Decode(&msg)
		if err != nil {
			panic(err)
		}
		// fmt.Println(msg)
		lagQueue <- msg
	}
}
