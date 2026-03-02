package repositories

import (
	"context"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/clients/database"
	"football-tracker/internal/models"
	"log/slog"
)

type championshipRepository struct {
	db *database.PostgresClient
}

func NewChampionshipRepository(dbClient *database.PostgresClient) ChampionshipRepository {
	return &championshipRepository{
		db: dbClient,
	}
}

func (r *championshipRepository) GetAll(ctx context.Context) ([]models.Championship, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying all championships from database",
		slog.String("component", "repository"),
		slog.String("method", "GetAll"),
	)

	query := `
		SELECT id, name, year, start_date, end_date, logo_url, created_at, updated_at
		FROM championships
		ORDER BY year DESC, start_date DESC
	`

	rows, err := r.db.DB.QueryContext(ctx, query)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to execute query",
			slog.String("component", "repository"),
			slog.String("method", "GetAll"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query championships: %w", err)
	}
	defer rows.Close()

	var championships []models.Championship

	for rows.Next() {
		var c models.Championship
		err := rows.Scan(
			&c.ID,
			&c.Name,
			&c.Year,
			&c.StartDate,
			&c.EndDate,
			&c.LogoURL,
			&c.CreatedAt,
			&c.UpdatedAt,
		)
		if err != nil {
			logger.GetLogger().Error(
				ctx,
				"Failed to scan championship row",
				slog.String("component", "repository"),
				slog.String("method", "GetAll"),
				slog.String("error", err.Error()),
			)
			return nil, fmt.Errorf("failed to scan championship: %w", err)
		}
		championships = append(championships, c)
	}

	if err = rows.Err(); err != nil {
		logger.GetLogger().Error(
			ctx,
			"Error iterating championship rows",
			slog.String("component", "repository"),
			slog.String("method", "GetAll"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("error iterating championships: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully queried championships from database",
		slog.String("component", "repository"),
		slog.String("method", "GetAll"),
		slog.Int("count", len(championships)),
	)

	return championships, nil
}

func (r *championshipRepository) GetByChampionshipId(ctx context.Context, championshipId int) (models.Championship, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting championship by Id",
		slog.String("component", "repository"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
	)

	query := `
		SELECT id, name, year, start_date, end_date, logo_url, created_at, updated_at
		FROM championships
		WHERE id = $1
	`

	var championship models.Championship
	err := r.db.DB.QueryRowContext(ctx, query, championshipId).Scan(
		&championship.ID,
		&championship.Name,
		&championship.Year,
		&championship.StartDate,
		&championship.EndDate,
		&championship.LogoURL,
		&championship.CreatedAt,
		&championship.UpdatedAt,
	)

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get championship by ID",
			slog.String("component", "repository"),
			slog.String("method", "GetByChampionshipId"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return models.Championship{}, fmt.Errorf("failed to get championship by ID %d: %w", championshipId, err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved championship by ID",
		slog.String("component", "repository"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
		slog.String("championship_name", championship.Name),
	)

	return championship, nil
}
