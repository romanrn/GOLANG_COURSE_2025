package services

import (
	"football-tracker/cmd/server/logger"
)

// init is called automatically before any tests run
// This ensures logger is initialized for all tests in this package
func init() {
	logger.InitTestLogger()
}
