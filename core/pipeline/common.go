package pipeline

import (
	"github.com/HarbinZhang/goRainbow/core/utils"
	"go.uber.org/zap"
)

var logger *zap.Logger

func PrepareLogger() {
	logger = utils.GetLogger()
}
