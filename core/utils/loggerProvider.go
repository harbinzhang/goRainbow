package utils

import (
	"encoding/json"

	"go.uber.org/zap"
)

// GetLogger returns a zap.Logger, need to take care of Sync() before exiting.
func GetLogger() *zap.Logger {
	rawJSON := []byte(`{
		"level": "debug",
		"encoding": "json",
		"outputPaths": ["./rainbow_log"],
		"errorOutputPaths": ["stderr"],
		"initialFields": {"service": "goRainbow"},
		"encoderConfig": {
		  "messageKey": "message",
		  "levelKey": "level",
		  "levelEncoder": "lowercase"
		}
	  }`)

	var cfg zap.Config
	if err := json.Unmarshal(rawJSON, &cfg); err != nil {
		panic(err)
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	return logger
}
