package database

import (
	"context"
	"database/sql"
	"fmt"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/logger"
	"log/slog"
	"time"

	_ "github.com/lib/pq"
)

type PostgresClient struct {
	DB *sql.DB
}

func NewPostgresClient(ctx context.Context, cfg *config.ServerConfig) (*PostgresClient, error) {
	logger.GetLogger().Info(
		ctx,
		"Initializing PostgreSQL client",
		slog.String("host", cfg.DbHost),
		slog.String("port", cfg.DbPort),
		slog.String("database", cfg.DbName),
		slog.String("user", cfg.DbUserName),
	)

	dsn := buildPostgresDSN(cfg)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to open PostgreSQL connection",
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	logger.GetLogger().Info(ctx, "PostgreSQL connection opened successfully")

	// Configure connection pool
	db.SetMaxOpenConns(int(cfg.MaxPoolSize))
	db.SetMaxIdleConns(int(cfg.MinPoolSize))
	db.SetConnMaxIdleTime(cfg.MaxConnIdleTime)
	db.SetConnMaxLifetime(cfg.MaxConnLifetime)

	logger.GetLogger().Info(
		ctx,
		"PostgreSQL connection pool configured",
		slog.Int("maxOpenConns", int(cfg.MaxPoolSize)),
		slog.Int("maxIdleConns", int(cfg.MinPoolSize)),
		slog.Duration("maxConnIdleTime", cfg.MaxConnIdleTime),
		slog.Duration("maxConnLifetime", cfg.MaxConnLifetime),
	)

	// Test connection
	logger.GetLogger().Info(ctx, "Testing PostgreSQL connection...")

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to ping PostgreSQL database",
			slog.String("error", err.Error()),
		)
		if closeErr := db.Close(); closeErr != nil {
			logger.GetLogger().Error(
				ctx,
				"Error closing database after failed ping",
				slog.String("error", closeErr.Error()),
			)
		}
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	logger.GetLogger().Info(ctx, "PostgreSQL connection established successfully")

	return &PostgresClient{
		DB: db,
	}, nil
}

func buildPostgresDSN(cfg *config.ServerConfig) string {
	// If DbUri is provided, use it
	if cfg.DbUri != "" {
		return cfg.DbUri
	}

	// Build DSN from components
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DbHost,
		cfg.DbPort,
		cfg.DbUserName,
		cfg.DbPsw,
		cfg.DbName,
	)
}

func (c *PostgresClient) Close() error {
	if c.DB != nil {
		logger.GetLogger().Info(context.Background(), "Closing PostgreSQL connection...")
		err := c.DB.Close()
		if err != nil {
			logger.GetLogger().Error(
				context.Background(),
				"Error closing PostgreSQL connection",
				slog.String("error", err.Error()),
			)
			return err
		}
		logger.GetLogger().Info(context.Background(), "PostgreSQL connection closed successfully")
		return nil
	}
	return nil
}

func (c *PostgresClient) Ping(ctx context.Context) error {
	logger.GetLogger().Info(ctx, "Pinging PostgreSQL database...")
	err := c.DB.PingContext(ctx)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"PostgreSQL ping failed",
			slog.String("error", err.Error()),
		)
		return err
	}
	logger.GetLogger().Info(ctx, "PostgreSQL ping successful")
	return nil
}

func (c *PostgresClient) GetDB() *sql.DB {
	return c.DB
}
