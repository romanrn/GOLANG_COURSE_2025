package otel

import (
	"context"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// ContextKey is the type for context keys used across the application
type ContextKey string

const (
	// TraceIDKey is the context key for trace ID
	// Set by OTEL middleware, used by logger for enrichment
	TraceIDKey ContextKey = "trace_id"

	// SpanIDKey is the context key for span ID
	// Set by OTEL middleware, used by logger for enrichment
	SpanIDKey ContextKey = "span_id"

	// UserIDKey is the context key for user ID
	// Set by auth middleware, used by logger for enrichment
	UserIDKey ContextKey = "user_id"
)

// Middleware creates OpenTelemetry spans and sends them to Tempo
// Supports W3C Trace Context propagation for distributed tracing:
// - If incoming request has "traceparent" header → creates child span with same trace_id
// - If no header → generates new trace_id
// - Extracts trace_id/span_id and stores in context for logger
type Middleware struct {
	tracer     oteltrace.Tracer
	propagator propagation.TextMapPropagator
}

func NewMiddleware(serviceName string) *Middleware {
	return &Middleware{
		tracer:     otel.Tracer(serviceName),
		propagator: otel.GetTextMapPropagator(), // W3C Trace Context propagator
	}
}

func (m *Middleware) Handle(c *fiber.Ctx) error {
	ctx := c.UserContext()

	// Extract trace context from HTTP headers (W3C Trace Context propagation)
	// This allows distributed tracing across microservices
	// Header format: "traceparent: 00-<trace_id>-<parent_span_id>-<flags>"
	ctx = m.propagator.Extract(ctx, &fiberHeaderCarrier{c: c})

	// Start span - will use extracted trace_id if present, or generate new one
	// If trace_id exists in context → creates child span
	// If no trace_id → creates root span with new trace_id
	ctx, span := m.tracer.Start(ctx, c.Method()+" "+c.Path(),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		oteltrace.WithAttributes(
			semconv.HTTPMethod(c.Method()),
			semconv.HTTPTarget(c.Path()),
			semconv.HTTPRoute(c.Route().Path),
			semconv.HTTPScheme(c.Protocol()),
			semconv.NetHostName(c.Hostname()),
			attribute.String("http.user_agent", string(c.Request().Header.UserAgent())),
			attribute.Int("http.request_content_length", len(c.Request().Body())),
		),
	)
	defer span.End()

	// Extract trace_id and span_id from span context
	spanCtx := span.SpanContext()
	if spanCtx.IsValid() {
		traceID := spanCtx.TraceID().String()
		spanID := spanCtx.SpanID().String()

		// Store in Go context for logger
		ctx = context.WithValue(ctx, TraceIDKey, traceID)
		ctx = context.WithValue(ctx, SpanIDKey, spanID)

		// Set response headers for debugging and downstream propagation
		c.Set("X-Trace-ID", traceID)
		c.Set("X-Span-ID", spanID)

		// Inject trace context into response headers for downstream services
		m.propagator.Inject(ctx, &fiberHeaderCarrier{c: c})
	}

	// Update Fiber's user context
	c.SetUserContext(ctx)

	// Call next handler
	err := c.Next()

	// Record response status
	status := c.Response().StatusCode()
	span.SetAttributes(
		semconv.HTTPStatusCode(status),
		attribute.Int("http.response_content_length", len(c.Response().Body())),
	)

	// Set span status
	if status >= 400 {
		span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.String("error.message", err.Error()))
		}
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// fiberHeaderCarrier adapts Fiber context to propagation.TextMapCarrier interface
// This allows OTEL propagator to extract/inject trace context from/to HTTP headers
type fiberHeaderCarrier struct {
	c *fiber.Ctx
}

// Get returns the value associated with the passed key from HTTP headers
func (f *fiberHeaderCarrier) Get(key string) string {
	return f.c.Get(key)
}

// Set stores the key-value pair in HTTP headers
func (f *fiberHeaderCarrier) Set(key string, value string) {
	f.c.Set(key, value)
}

// Keys lists all headers (required by TextMapCarrier interface)
func (f *fiberHeaderCarrier) Keys() []string {
	keys := make([]string, 0)
	f.c.Request().Header.VisitAll(func(key, _ []byte) {
		keys = append(keys, string(key))
	})
	return keys
}
