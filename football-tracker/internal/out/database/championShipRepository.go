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
