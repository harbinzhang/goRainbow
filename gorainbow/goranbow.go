package main

import (
	"encoding/json"
	"net/http"

	"github.com/HarbinZhang/goRainbow/producer"
)

// Field represents a Field
type Field struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short,omitempty"`
}

// LagMessage is a simulator for Burrow lag message
type LagMessage struct {
	Attachments []struct {
		Color  string  `json:"color"`
		Title  string  `json:"title"`
		Fields []Field `json:"fields"`
	} `json:"attachments"`
}

func main() {
	lagQueue := make(chan LagMessage, 1000)
	go producer.Produce(lagQueue)
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
