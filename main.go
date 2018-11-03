package main

import (
	"encoding/json"
	"net/http"
)

func main() {
	lagQueue := make(chan LagMessage, 1000)
	go produce(lagQueue)
	lagHandler := consumeLag(lagQueue)
	http.HandleFunc("/rainbow/lag", lagHandler)
	http.ListenAndServe(":7099", nil)
}

func consumeLag(lagQueue chan LagMessage) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var msg LagMessage
		err := decoder.Decode(&msg)
		if err != nil {
			panic(err)
		}
		lagQueue <- msg
	}
}
