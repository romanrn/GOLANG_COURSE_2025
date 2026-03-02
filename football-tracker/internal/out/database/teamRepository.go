package repositories

import (
	"context"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/clients/database"
	"football-tracker/internal/models"
	"log/slog"
)

type teamRepository struct {
	db *database.PostgresClient
}

func NewTeamRepository(dbClient *database.PostgresClient) TeamRepository {
	return &teamRepository{
		db: dbClient,
	}
}

func (r *teamRepository) GetByChampionshipId(ctx context.Context, championshipID int) ([]models.Team, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying teams for championship from database",
		slog.String("component", "repository"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipID),
	)

	query := `
		SELECT t.id, t.name, t.country_code, t.country_name, t.flag_url, t.created_at, t.updated_at
		FROM teams t
		INNER JOIN championship_teams ct ON t.id = ct.team_id
		WHERE ct.championship_id = $1
		ORDER BY t.name ASC
	`

	rows, err := r.db.DB.QueryContext(ctx, query, championshipID)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to execute query",
			slog.String("component", "repository"),
			slog.String("method", "GetByChampionshipID"),
			slog.Int("championship_id", championshipID),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query teams: %w", err)
	}
	defer rows.Close()

	var teams []models.Team

	for rows.Next() {
		var t models.Team
		err := rows.Scan(
			&t.ID,
			&t.Name,
			&t.CountryCode,
			&t.CountryName,
			&t.FlagURL,
			&t.CreatedAt,
			&t.UpdatedAt,
		)
		if err != nil {
			logger.GetLogger().Error(
				ctx,
				"Failed to scan team row",
				slog.String("component", "repository"),
				slog.String("method", "GetByChampionshipID"),
				slog.String("error", err.Error()),
			)
			return nil, fmt.Errorf("failed to scan team: %w", err)
		}
		teams = append(teams, t)
	}

	if err = rows.Err(); err != nil {
		logger.GetLogger().Error(
			ctx,
			"Error iterating team rows",
			slog.String("component", "repository"),
			slog.String("method", "GetByChampionshipID"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("error iterating teams: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully queried teams from database",
		slog.String("component", "repository"),
		slog.String("method", "GetByChampionshipID"),
		slog.Int("championship_id", championshipID),
		slog.Int("count", len(teams)),
	)

	return teams, nil
}

func (r *teamRepository) GetById(ctx context.Context, teamId int) (models.Team, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying team by Id from database",
		slog.String("component", "repository"),
		slog.String("method", "GetById"),
		slog.Int("team_id", teamId),
	)

	query := `
		SELECT id, name, country_code, country_name, flag_url, created_at, updated_at
		FROM teams
		WHERE id = $1
	`

	var team models.Team
	err := r.db.DB.QueryRowContext(ctx, query, teamId).Scan(
		&team.ID,
		&team.Name,
		&team.CountryCode,
		&team.CountryName,
		&team.FlagURL,
		&team.CreatedAt,
		&team.UpdatedAt,
	)

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get team by ID",
			slog.String("component", "repository"),
			slog.String("method", "GetById"),
			slog.Int("team_id", teamId),
			slog.String("error", err.Error()),
		)
		return models.Team{}, fmt.Errorf("failed to get team by ID %d: %w", teamId, err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved team by ID",
		slog.String("component", "repository"),
		slog.String("method", "GetById"),
		slog.Int("team_id", teamId),
		slog.String("team_name", team.Name),
	)

	return team, nil
}
