package testhelpers

import (
	"context"
	"testing"
	"time"
)

// ContextWithTimeout creates a context with timeout and automatic cleanup
func ContextWithTimeout(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

// ContextWithDeadline creates a context with deadline and automatic cleanup
func ContextWithDeadline(t *testing.T, deadline time.Time) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	t.Cleanup(cancel)
	return ctx
}

// ContextWithCancel creates a cancelable context and automatic cleanup
func ContextWithCancel(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx, cancel
}

// ContextWithTraceID creates a context with trace ID for testing distributed tracing
func ContextWithTraceID(traceID string) context.Context {
	ctx := context.Background()
	// Add trace ID to context (adjust the key based on your OTEL setup)
	ctx = context.WithValue(ctx, "trace_id", traceID)
	ctx = context.WithValue(ctx, "span_id", "test-span-id")
	return ctx
}

// ContextWithUserID creates a context with user ID for testing auth
func ContextWithUserID(userID int) context.Context {
	ctx := context.Background()
	return context.WithValue(ctx, "user_id", userID)
}

// ContextWithValues creates a context with multiple test values
func ContextWithValues(values map[string]interface{}) context.Context {
	ctx := context.Background()
	for key, value := range values {
		ctx = context.WithValue(ctx, key, value)
	}
	return ctx
}

// DefaultTestContext returns a context with 5 second timeout for tests
func DefaultTestContext(t *testing.T) context.Context {
	return ContextWithTimeout(t, 5*time.Second)
}
