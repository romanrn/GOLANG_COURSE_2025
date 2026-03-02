package jobs

import (
	"context"
	"football-tracker/cmd/server/logger"
	repositories "football-tracker/internal/out/database"
	"log/slog"
	"time"
)

// SessionCleaner periodically removes expired sessions from database
type SessionCleaner struct {
	sessionRepo repositories.SessionRepository
	interval    time.Duration
	stopChan    chan struct{}
}

// NewSessionCleaner creates a new session cleaner job
func NewSessionCleaner(sessionRepo repositories.SessionRepository, interval time.Duration) *SessionCleaner {
	return &SessionCleaner{
		sessionRepo: sessionRepo,
		interval:    interval,
		stopChan:    make(chan struct{}),
	}
}

// Start begins the periodic cleanup process
func (sc *SessionCleaner) Start(ctx context.Context) {
	ticker := time.NewTicker(sc.interval)
	defer ticker.Stop()

	logger.GetLogger().Info(
		ctx,
		"Session cleaner started",
		slog.String("component", "job"),
		slog.String("method", "Start"),
		slog.Duration("interval", sc.interval),
	)

	// Run cleanup immediately on startup
	sc.cleanup(ctx)

	for {
		select {
		case <-ticker.C:
			sc.cleanup(ctx)
		case <-sc.stopChan:
			logger.GetLogger().Info(
				ctx,
				"Session cleaner stopped",
				slog.String("component", "job"),
				slog.String("method", "Start"),
			)
			return
		case <-ctx.Done():
			logger.GetLogger().Info(
				ctx,
				"Session cleaner context cancelled",
				slog.String("component", "job"),
				slog.String("method", "Start"),
			)
			return
		}
	}
}

// Stop gracefully stops the cleaner
func (sc *SessionCleaner) Stop() {
	close(sc.stopChan)
}

// GetName returns the job name
func (sc *SessionCleaner) GetName() string {
	return "session_cleaner"
}

// cleanup performs the actual cleanup operation
func (sc *SessionCleaner) cleanup(ctx context.Context) {
	logger.GetLogger().Debug(
		ctx,
		"Starting expired sessions cleanup",
		slog.String("component", "job"),
		slog.String("method", "cleanup"),
	)

	err := sc.sessionRepo.DeleteExpired(ctx)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to cleanup expired sessions",
			slog.String("component", "job"),
			slog.String("method", "cleanup"),
			slog.String("error", err.Error()),
		)
		return
	}

	logger.GetLogger().Debug(
		ctx,
		"Expired sessions cleanup completed",
		slog.String("component", "job"),
		slog.String("method", "cleanup"),
	)
}
