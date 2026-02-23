package trace

import (
	"football-tracker/cmd/server/config"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"
)

type contextKey string

const (
	traceIDKey contextKey = "trace_id"
	spanIDKey  contextKey = "span_id"
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
	// Get the current span context from the request context
	spanCtx := trace.SpanContextFromContext(ctx.UserContext())

	// Extract trace ID and span ID if they exist
	if spanCtx.IsValid() {
		traceID := spanCtx.TraceID().String()
		spanID := spanCtx.SpanID().String()

		// Store trace ID and span ID in the Fiber context
		ctx.Locals(traceIDKey, traceID)
		ctx.Locals(spanIDKey, spanID)

		// Also set them as response headers for debugging
		ctx.Set("X-Trace-ID", traceID)
		ctx.Set("X-Span-ID", spanID)
	}
	// Call next handler
	return ctx.Next()
}

// GetTraceID extracts trace_id from Fiber context
func GetTraceID(ctx *fiber.Ctx) string {
	if traceID, ok := ctx.Locals(traceIDKey).(string); ok {
		return traceID
	}
	return ""
}

// GetSpanID extracts span_id from Fiber context
func GetSpanID(ctx *fiber.Ctx) string {
	if spanID, ok := ctx.Locals(spanIDKey).(string); ok {
		return spanID
	}
	return ""
}
