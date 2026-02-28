package middlewares

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/middlewares/auth"
	"football-tracker/cmd/server/middlewares/errorHandler"
	"football-tracker/cmd/server/middlewares/logger"
	"football-tracker/cmd/server/middlewares/trace"
	"football-tracker/internal/services"
)

type Middlewares struct {
	Trace        *trace.Middleware
	ErrorHandler *errorHandler.Middleware
	Logger       *logger.Middleware
	Auth         *auth.Middleware
}

func NewMiddlewares(svc *services.Services, cfg *config.ServerConfig) *Middlewares {
	return &Middlewares{
		Trace:        trace.NewMiddleware(cfg),
		ErrorHandler: errorHandler.NewMiddleware(),
		Logger:       logger.NewMiddleware(cfg),
		Auth:         auth.NewAuthMiddleware(svc.Auth),
	}
}
