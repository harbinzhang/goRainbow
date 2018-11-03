package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/HarbinZhang/stringutil"
)

func main() {
	lagQueue := make(chan LagMessage, 1000)
	go rainbowProducer.producer(lagQueue)
	lagHandler := consumeLag(lagQueue)
	http.HandleFunc("/rainbow/lag", lagHandler)
	http.ListenAndServe(":7099", nil)
	fmt.Printf(stringutil.Reverse("!oG ,olleH"))
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
