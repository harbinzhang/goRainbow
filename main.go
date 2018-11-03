package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/HarbinZhang/goRainbow/config"
)

func main() {

	// go rainbowProducer.producer(lagQueue)
	http.HandleFunc("/rainbow/lag", lagHandler)
	http.ListenAndServe(":7099", nil)

}

func lagHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var msg config.LagMessage
	err := decoder.Decode(&msg)
	if err != nil {
		panic(err)
	}
	fmt.Print(msg)
}
