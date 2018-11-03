package core

import (
	"fmt"

	"github.com/HarbinZhang/goRainbow/config"
)

// Translator for message translate from struct to string
func Translator(lagQueue chan config.LagMessage) {
	select {
	case lag := <-lagQueue:
		fmt.Println("trans: ", lag)
	}

}
