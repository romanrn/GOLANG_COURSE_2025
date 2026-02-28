package repositories

import (
	"context"
	"database/sql"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/clients/database"
	"football-tracker/internal/models"
	"log/slog"
)

type sessionRepository struct {
	db *database.PostgresClient
}

func NewSessionRepository(dbClient *database.PostgresClient) SessionRepository {
	return &sessionRepository{
		db: dbClient,
	}
}

func (r *sessionRepository) Create(ctx context.Context, session *models.Session) error {
	logger.GetLogger().Info(
		ctx,
		"Creating new session",
		slog.String("component", "repository"),
		slog.String("method", "Create"),
		slog.Int("user_id", session.UserID),
	)

	query := `
		INSERT INTO sessions (user_id, token, expires_at, created_at)
		VALUES ($1, $2, $3, NOW())
		RETURNING id, created_at
	`

	err := r.db.DB.QueryRowContext(
		ctx,
		query,
		session.UserID,
		session.Token,
		session.ExpiresAt,
	).Scan(&session.ID, &session.CreatedAt)

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to create session",
			slog.String("component", "repository"),
			slog.String("method", "Create"),
			slog.Int("user_id", session.UserID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to create session: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Session created successfully",
		slog.String("component", "repository"),
		slog.String("method", "Create"),
		slog.Int("session_id", session.ID),
	)

	return nil
}

func (r *sessionRepository) GetByToken(ctx context.Context, token string) (*models.Session, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying session by token",
		slog.String("component", "repository"),
		slog.String("method", "GetByToken"),
	)

	query := `
		SELECT id, user_id, token, expires_at, created_at
		FROM sessions
		WHERE token = $1
	`

	var session models.Session
	err := r.db.DB.QueryRowContext(ctx, query, token).Scan(
		&session.ID,
		&session.UserID,
		&session.Token,
		&session.ExpiresAt,
		&session.CreatedAt,
	)

	if err == sql.ErrNoRows {
		logger.GetLogger().Info(
			ctx,
			"Session not found by token",
			slog.String("component", "repository"),
			slog.String("method", "GetByToken"),
		)
		return nil, nil
	}

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to query session by token",
			slog.String("component", "repository"),
			slog.String("method", "GetByToken"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query session: %w", err)
	}

	return &session, nil
}

func (r *sessionRepository) DeleteByToken(ctx context.Context, token string) error {
	logger.GetLogger().Info(
		ctx,
		"Deleting session by token",
		slog.String("component", "repository"),
		slog.String("method", "DeleteByToken"),
	)

	query := `DELETE FROM sessions WHERE token = $1`

	result, err := r.db.DB.ExecContext(ctx, query, token)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to delete session",
			slog.String("component", "repository"),
			slog.String("method", "DeleteByToken"),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to delete session: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	logger.GetLogger().Info(
		ctx,
		"Session deleted",
		slog.String("component", "repository"),
		slog.String("method", "DeleteByToken"),
		slog.Int64("rows_affected", rowsAffected),
	)

	return nil
}

func (r *sessionRepository) DeleteByUserId(ctx context.Context, userId int) error {
	logger.GetLogger().Info(
		ctx,
		"Deleting all sessions for user",
		slog.String("component", "repository"),
		slog.String("method", "DeleteByUserId"),
		slog.Int("user_id", userId),
	)

	query := `DELETE FROM sessions WHERE user_id = $1`

	result, err := r.db.DB.ExecContext(ctx, query, userId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to delete user sessions",
			slog.String("component", "repository"),
			slog.String("method", "DeleteByUserId"),
			slog.Int("user_id", userId),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to delete user sessions: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	logger.GetLogger().Info(
		ctx,
		"User sessions deleted",
		slog.String("component", "repository"),
		slog.String("method", "DeleteByUserId"),
		slog.Int("user_id", userId),
		slog.Int64("rows_affected", rowsAffected),
	)

	return nil
}

func (r *sessionRepository) DeleteExpired(ctx context.Context) error {
	logger.GetLogger().Info(
		ctx,
		"Deleting expired sessions",
		slog.String("component", "repository"),
		slog.String("method", "DeleteExpired"),
	)

	query := `DELETE FROM sessions WHERE expires_at < NOW()`

	result, err := r.db.DB.ExecContext(ctx, query)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to delete expired sessions",
			slog.String("component", "repository"),
			slog.String("method", "DeleteExpired"),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to delete expired sessions: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	logger.GetLogger().Info(
		ctx,
		"Expired sessions deleted",
		slog.String("component", "repository"),
		slog.String("method", "DeleteExpired"),
		slog.Int64("rows_affected", rowsAffected),
	)

	return nil
}
