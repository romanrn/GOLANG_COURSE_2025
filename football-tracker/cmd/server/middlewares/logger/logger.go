package logger

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/logger"
	"football-tracker/cmd/server/middlewares/trace"
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

	traceID := trace.GetTraceID(ctx)
	spanID := trace.GetSpanID(ctx)

	if traceID != "" {
		WithLoggerAttrs(ctx,
			slog.String("trace_id", traceID),
			slog.String("span_id", spanID),
		)
	}

	logger.GetLogger().Info(
		ctx.Context(),
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
		logger.GetLogger().Error(ctx.Context(), "request end", attrs...)
	case statusCode >= 400:
		logger.GetLogger().Warn(ctx.Context(), "request end", attrs...)
	default:
		logger.GetLogger().Info(ctx.Context(), "request end", attrs...)
	}

	return err
}
