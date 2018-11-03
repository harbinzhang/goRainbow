package main

import (
	"encoding/json"
	"net/http"

	"github.com/HarbinZhang/goRainbow/config"
	"github.com/HarbinZhang/goRainbow/producer"
)

func main() {
	lagQueue := make(chan config.LagMessage, 1000)
	go producer.Produce(lagQueue)
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
		lagQueue <- msg
	}
}
