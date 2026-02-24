package logger

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/logger"
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
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

	statusCode := ctx.Response().StatusCode()
	attrs := append(getLoggerAttrs(ctx), slog.Int("status_code", statusCode))

	switch {
	case statusCode >= 500:
		logger.GetLogger().Error(userCtx, "request end", attrs...)
	case statusCode >= 400:
		logger.GetLogger().Warn(userCtx, "request end", attrs...)
	default:
		logger.GetLogger().Info(userCtx, "request end", attrs...)
	}

	return err
}
