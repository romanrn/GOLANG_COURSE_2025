package services

import (
	"context"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/models"
	repositories "football-tracker/internal/out/database"
	"log/slog"
)

type championshipService struct {
	repo repositories.ChampionshipRepository
}

func NewChampionshipService(repo repositories.ChampionshipRepository) ChampionshipService {
	return &championshipService{
		repo: repo,
	}
}

func (s *championshipService) GetAll(ctx context.Context) ([]models.Championship, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting all championships",
		slog.String("component", "service"),
		slog.String("method", "GetAll"),
	)

	championships, err := s.repo.GetAll(ctx)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get all championships",
			slog.String("component", "service"),
			slog.String("method", "GetAll"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to get all championships: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved championships",
		slog.String("component", "service"),
		slog.String("method", "GetAll"),
		slog.Int("count", len(championships)),
	)

	return championships, nil
}

func (s *championshipService) GetByChampionshipId(ctx context.Context, championshipId int) (models.Championship, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting championship by Id",
		slog.String("component", "service"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
	)

	championship, err := s.repo.GetByChampionshipId(ctx, championshipId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get championship by ID",
			slog.String("component", "service"),
			slog.String("method", "GetByChampionshipId"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return models.Championship{}, fmt.Errorf("failed to get championship by ID: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved championship by ID",
		slog.String("component", "service"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
		slog.String("championship_name", championship.Name),
	)

	return championship, nil
}
