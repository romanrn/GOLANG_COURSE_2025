package services

import (
	"context"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/models"
	repositories "football-tracker/internal/out/database"
	"log/slog"
)

type teamService struct {
	repo repositories.TeamRepository
}

func NewTeamService(repo repositories.TeamRepository) TeamService {
	return &teamService{
		repo: repo,
	}
}

func (s *teamService) GetByChampionshipId(ctx context.Context, championshipId int) ([]models.Team, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting teams for championship",
		slog.String("component", "service"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
	)

	teams, err := s.repo.GetByChampionshipId(ctx, championshipId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get teams from repository",
			slog.String("component", "service"),
			slog.String("method", "GetByChampionshipId"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to get teams for championship: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved teams for championship",
		slog.String("component", "service"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
		slog.Int("count", len(teams)),
	)

	return teams, nil
}

func (s *teamService) GetById(ctx context.Context, teamId int) (models.Team, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting team by Id",
		slog.String("component", "service"),
		slog.String("method", "GetById"),
		slog.Int("team_id", teamId),
	)

	team, err := s.repo.GetById(ctx, teamId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get team by Id",
			slog.String("component", "service"),
			slog.String("method", "GetById"),
			slog.Int("team_id", teamId),
			slog.String("error", err.Error()),
		)
		return models.Team{}, fmt.Errorf("failed to get team by Id: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved team by ID",
		slog.String("component", "service"),
		slog.String("method", "GetById"),
		slog.Int("team_id", teamId),
		slog.String("team_name", team.Name),
	)

	return team, nil
}
