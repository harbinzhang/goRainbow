package util

import (
	"encoding/json"

	"go.uber.org/zap"
)

// GetLogger returns a zap.Logger, need to take care of Sync() before exiting.
func GetLogger() *zap.Logger {
	rawJSON := []byte(`{
		"level": "warn",
		"encoding": "json",
		"outputPaths": ["stdout","./rainbow_log"],
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
		panic("Err decode logger config: " + err.Error())
	}

	logger, err := cfg.Build()
	if err != nil {
		panic("Err building logger config: " + err.Error())
	}

	return logger
}
