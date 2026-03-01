package logger

import (
	"io"
	"log/slog"
)

// InitTestLogger initializes a logger for testing with minimal configuration
// This should be called from test init() functions
func InitTestLogger() {
	if logger != nil {
		return // Already initialized
	}

	// Use a discard writer to suppress output during tests
	h := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError, // Only log errors in tests
	})

	logger = &Logger{
		cfg: nil, // No config needed for tests
		log: slog.New(h),
	}
}
