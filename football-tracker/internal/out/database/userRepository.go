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

type userRepository struct {
	db *database.PostgresClient
}

func NewUserRepository(dbClient *database.PostgresClient) UserRepository {
	return &userRepository{
		db: dbClient,
	}
}

func (r *userRepository) Create(ctx context.Context, user *models.User) error {
	logger.GetLogger().Info(
		ctx,
		"Creating new user in database",
		slog.String("component", "repository"),
		slog.String("method", "Create"),
		slog.String("username", user.Username),
		slog.String("email", user.Email),
	)

	query := `
		INSERT INTO users (username, email, password, role, created_at, updated_at)
		VALUES ($1, $2, $3, $4, NOW(), NOW())
		RETURNING id, created_at, updated_at
	`

	err := r.db.DB.QueryRowContext(
		ctx,
		query,
		user.Username,
		user.Email,
		user.Password,
		user.Role,
	).Scan(&user.ID, &user.CreatedAt, &user.UpdatedAt)

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to create user",
			slog.String("component", "repository"),
			slog.String("method", "Create"),
			slog.String("username", user.Username),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to create user: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"User created successfully",
		slog.String("component", "repository"),
		slog.String("method", "Create"),
		slog.Int("user_id", user.ID),
	)

	return nil
}

func (r *userRepository) GetByUsername(ctx context.Context, username string) (*models.User, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying user by username",
		slog.String("component", "repository"),
		slog.String("method", "GetByUsername"),
		slog.String("username", username),
	)

	query := `
		SELECT id, username, email, password, role, created_at, updated_at
		FROM users
		WHERE username = $1
	`

	var user models.User
	err := r.db.DB.QueryRowContext(ctx, query, username).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.Password,
		&user.Role,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		logger.GetLogger().Info(
			ctx,
			"User not found by username",
			slog.String("component", "repository"),
			slog.String("method", "GetByUsername"),
			slog.String("username", username),
		)
		return nil, nil
	}

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to query user by username",
			slog.String("component", "repository"),
			slog.String("method", "GetByUsername"),
			slog.String("username", username),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	return &user, nil
}

func (r *userRepository) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying user by email",
		slog.String("component", "repository"),
		slog.String("method", "GetByEmail"),
		slog.String("email", email),
	)

	query := `
		SELECT id, username, email, password, role, created_at, updated_at
		FROM users
		WHERE email = $1
	`

	var user models.User
	err := r.db.DB.QueryRowContext(ctx, query, email).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.Password,
		&user.Role,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		logger.GetLogger().Info(
			ctx,
			"User not found by email",
			slog.String("component", "repository"),
			slog.String("method", "GetByEmail"),
			slog.String("email", email),
		)
		return nil, nil
	}

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to query user by email",
			slog.String("component", "repository"),
			slog.String("method", "GetByEmail"),
			slog.String("email", email),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	return &user, nil
}

func (r *userRepository) GetById(ctx context.Context, userId int) (*models.User, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying user by ID",
		slog.String("component", "repository"),
		slog.String("method", "GetById"),
		slog.Int("user_id", userId),
	)

	query := `
		SELECT id, username, email, password, role, created_at, updated_at
		FROM users
		WHERE id = $1
	`

	var user models.User
	err := r.db.DB.QueryRowContext(ctx, query, userId).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.Password,
		&user.Role,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		logger.GetLogger().Info(
			ctx,
			"User not found by ID",
			slog.String("component", "repository"),
			slog.String("method", "GetById"),
			slog.Int("user_id", userId),
		)
		return nil, nil
	}

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to query user by ID",
			slog.String("component", "repository"),
			slog.String("method", "GetById"),
			slog.Int("user_id", userId),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	return &user, nil
}
