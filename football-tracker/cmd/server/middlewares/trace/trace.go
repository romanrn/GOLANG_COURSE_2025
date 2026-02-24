package trace

import (
	"context"
	"football-tracker/cmd/server/config"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// ContextKey is exported so logger can use the same keys
type ContextKey string

const (
	// TraceIDKey is the context key for trace ID (exported for logger)
	TraceIDKey ContextKey = "trace_id"
	// SpanIDKey is the context key for span ID (exported for logger)
	SpanIDKey ContextKey = "span_id"
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
	userCtx := ctx.UserContext()

	// Get the current span context from the request context
	spanCtx := trace.SpanContextFromContext(userCtx)

	var traceID, spanID string

	// Extract trace ID and span ID if they exist
	if spanCtx.IsValid() {
		traceID = spanCtx.TraceID().String()
		spanID = spanCtx.SpanID().String()
	} else {
		// Generate trace ID if not present (fallback)
		traceID = uuid.New().String()
		spanID = uuid.New().String()
	}

	// Store in Go context (for services/repositories)
	userCtx = context.WithValue(userCtx, TraceIDKey, traceID)
	userCtx = context.WithValue(userCtx, SpanIDKey, spanID)

	// Update Fiber's user context with enriched version
	ctx.SetUserContext(userCtx)

	// Set response headers for debugging
	ctx.Set("X-Trace-ID", traceID)
	ctx.Set("X-Span-ID", spanID)

	// Call next handler
	return ctx.Next()
}
