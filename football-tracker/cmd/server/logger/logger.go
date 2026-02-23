package logger

import (
	"context"
	"football-tracker/cmd/server/config"
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

type CtxLoggerKey struct{}

func (s *Logger) Info(ctx context.Context, msg string, args ...any) {
	s.log.InfoContext(ctx, msg, args...)
}

func (s *Logger) Error(ctx context.Context, msg string, args ...any) {
	s.log.ErrorContext(ctx, msg, args...)
}

func (s *Logger) Warn(ctx context.Context, msg string, args ...any) {
	s.log.WarnContext(ctx, msg, args...)
}

func (s *Logger) Fatal(ctx context.Context, msg string, args ...any) {
	s.log.ErrorContext(ctx, msg, args...)
	os.Exit(1)
}

/*
func WithAttrs(ctx context.Context, args ...slog.Attr) context.Context {
	ctx = context.WithValue(ctx, CtxLoggerKey{}, MergeAttrs(getAttrs(ctx), args))
	return ctx
}


func MergeAttrs(left []slog.Attr, right []slog.Attr) []slog.Attr {
	return append(left, right...)
}

func getAttrs(ctx context.Context) []slog.Attr {
	attrs := ctx.Value(CtxLoggerKey{})
	if attrs == nil {
		return []slog.Attr{}
	}
	result, ok := attrs.([]slog.Attr)
	if !ok {
		return []slog.Attr{}
	}
	return result
}

func convertAttrsToAny(a []slog.Attr) []any {
	result := make([]any, len(a))
	for i, v := range a {
		result[i] = v
	}
	return result
}

func (s *Logger) Info(ctx context.Context, msg string, args ...slog.Attr) {
	s.log.InfoContext(ctx, msg, convertAttrsToAny(MergeAttrs(getAttrs(ctx), args))...)
}

func (s *Logger) Error(ctx context.Context, err error, args ...slog.Attr) {
	s.log.ErrorContext(ctx, err.Error(), convertAttrsToAny(MergeAttrs(getAttrs(ctx), args))...)
}

func (s *Logger) Panic(ctx context.Context, err error, args ...slog.Attr) {
	s.log.ErrorContext(ctx, err.Error(), args)
	panic(err)
}

func (s *Logger) Fatal(ctx context.Context, err error, args ...slog.Attr) {
	s.log.ErrorContext(ctx, err.Error(), args)
	os.Exit(1)
}*/
