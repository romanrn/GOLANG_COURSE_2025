package logger

import (
	"errors"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/logger"
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

type Middleware struct {
	cfg *config.ServerConfig
}

func NewMiddleware(cfg *config.ServerConfig) *Middleware {
	return &Middleware{
		cfg: cfg,
	}
}

func (m *Middleware) Handle(ctx *fiber.Ctx) error {
	// Get user context with traceID/spanID added by trace middleware
	userCtx := ctx.UserContext()

	logger.GetLogger().Info(
		userCtx,
		"request start",
		getLoggerAttrs(ctx)...,
	)

	err := ctx.Next()

	var fe *fiber.Error
	if errors.As(err, &fe) {
		WithLoggerAttrs(ctx, slog.String("resp_message", fe.Message))
	} else if err != nil {
		WithLoggerAttrs(ctx, slog.String("resp_message", err.Error()))
	}

	// Get updated context after all middleware (includes user_id from Optional Auth)
	updatedCtx := ctx.UserContext()

	statusCode := ctx.Response().StatusCode()
	attrs := append(getLoggerAttrs(ctx), slog.Int("status_code", statusCode))

	switch {
	case statusCode >= 500:
		logger.GetLogger().Error(updatedCtx, "request end", attrs...)
	case statusCode >= 400:
		logger.GetLogger().Warn(updatedCtx, "request end", attrs...)
	default:
		logger.GetLogger().Info(updatedCtx, "request end", attrs...)
	}

	return err
}
