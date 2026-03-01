package middlewares

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/middlewares/auth"
	"football-tracker/cmd/server/middlewares/errorHandler"
	"football-tracker/cmd/server/middlewares/logger"
	"football-tracker/cmd/server/middlewares/otel"
	"football-tracker/internal/services"
)

// Note: trace package is still imported by logger and auth for context keys,
// but the trace middleware itself is no longer used (OTEL generates trace_id now)

type Middlewares struct {
	Otel         *otel.Middleware
	ErrorHandler *errorHandler.Middleware
	Logger       *logger.Middleware
	Auth         *auth.Middleware
}

func NewMiddlewares(svc *services.Services, cfg *config.ServerConfig) *Middlewares {
	return &Middlewares{
		Otel:         otel.NewMiddleware(cfg.OtelServiceName),
		ErrorHandler: errorHandler.NewMiddleware(),
		Logger:       logger.NewMiddleware(cfg),
		Auth:         auth.NewAuthMiddleware(svc.Auth),
	}
}
