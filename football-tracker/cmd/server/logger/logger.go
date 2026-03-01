package logger

import (
	"context"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/middlewares/otel"
	"log/slog"
	"os"
)

var logger *Logger

func GetLogger() *Logger {
	return logger
}

type Logger struct {
	cfg *config.ServerConfig
	log *slog.Logger
}

func InitLogger(cfg *config.ServerConfig) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: convertToSlogLevel(cfg.LoggerLevel),
	})
	logger = &Logger{
		cfg: cfg,
		log: slog.New(h),
	}
}

func convertToSlogLevel(level string) slog.Leveler {
	switch level {
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "debug":
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

// enrichArgs automatically adds traceID, spanID, and userID from context
func enrichArgs(ctx context.Context, args []any) []any {
	enriched := make([]any, 0, len(args)+6)

	// Add traceID and spanID if present in context using otel package keys
	if traceID, ok := ctx.Value(otel.TraceIDKey).(string); ok && traceID != "" {
		enriched = append(enriched, slog.String("trace_id", traceID))
	}
	if spanID, ok := ctx.Value(otel.SpanIDKey).(string); ok && spanID != "" {
		enriched = append(enriched, slog.String("span_id", spanID))
	}

	// Add userID if present in context (from auth middleware)
	if userID, ok := ctx.Value(otel.UserIDKey).(int); ok && userID > 0 {
		enriched = append(enriched, slog.Int("user_id", userID))
	}

	// Add original arguments
	enriched = append(enriched, args...)

	return enriched
}

func (s *Logger) Debug(ctx context.Context, msg string, args ...any) {
	s.log.DebugContext(ctx, msg, enrichArgs(ctx, args)...)
}

func (s *Logger) Info(ctx context.Context, msg string, args ...any) {
	s.log.InfoContext(ctx, msg, enrichArgs(ctx, args)...)
}

func (s *Logger) Error(ctx context.Context, msg string, args ...any) {
	s.log.ErrorContext(ctx, msg, enrichArgs(ctx, args)...)
}

func (s *Logger) Warn(ctx context.Context, msg string, args ...any) {
	s.log.WarnContext(ctx, msg, enrichArgs(ctx, args)...)
}

func (s *Logger) Fatal(ctx context.Context, msg string, args ...any) {
	s.log.ErrorContext(ctx, msg, enrichArgs(ctx, args)...)
	os.Exit(1)
}
