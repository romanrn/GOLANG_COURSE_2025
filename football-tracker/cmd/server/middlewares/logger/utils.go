package logger

import (
	"github.com/gofiber/fiber/v2"
)

type contextKey string

const loggerAttrsKey contextKey = "logger_attrs"

func WithLoggerAttrs(ctx *fiber.Ctx, attrs ...any) {
	existing := getLoggerAttrs(ctx)
	existing = append(existing, attrs...)
	ctx.Locals(loggerAttrsKey, existing)
}

func GetLoggerAttrs(ctx *fiber.Ctx) []any {
	return getLoggerAttrs(ctx)
}

func getLoggerAttrs(ctx *fiber.Ctx) []any {
	attrs, ok := ctx.Locals(loggerAttrsKey).([]any)
	if !ok {
		return []any{}
	}
	return attrs
}
