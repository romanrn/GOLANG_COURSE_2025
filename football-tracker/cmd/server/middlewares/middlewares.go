package middlewares

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/middlewares/errorHandler"
	"football-tracker/cmd/server/middlewares/logger"
	"football-tracker/cmd/server/middlewares/trace"
)

type Middlewares struct {
	Trace        *trace.Middleware
	ErrorHandler *errorHandler.Middleware
	Logger       *logger.Middleware
}

func NewMiddlewares(cfg *config.ServerConfig) *Middlewares {
	return &Middlewares{
		Trace:        trace.NewMiddleware(cfg),
		ErrorHandler: errorHandler.NewMiddleware(),
		Logger:       logger.NewMiddleware(cfg),
	}
}

/*func NewMiddlewares(svc *services.Services, cfg *config.ServerConfig) *Middlewares {
	return &Middlewares{
		Trace:  trace.NewMiddleware(cfg),
		Logger: logger.NewMiddleware(svc, cfg),
	}
}*/
