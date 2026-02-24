package services

import (
	"context"
	"fmt"
	"football-tracker/cmd/server/logger"
	repositories "football-tracker/internal/out/database"
	"log/slog"
)

type matchService struct {
	repo repositories.MatchRepository
}

func NewMatchService(repo repositories.MatchRepository) MatchService {
	return &matchService{
		repo: repo,
	}
}

func (s *matchService) GetByID(ctx context.Context, id string) (string, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting match by ID",
		slog.String("component", "service"),
		slog.String("method", "GetByID"),
		slog.String("match_id", id),
	)

	_, err := s.repo.GetByID(ctx, id)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get match by ID",
			slog.String("component", "service"),
			slog.String("method", "GetByID"),
			slog.String("match_id", id),
			slog.String("error", err.Error()),
		)
		return "", fmt.Errorf("failed to get match: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved match",
		slog.String("component", "service"),
		slog.String("method", "GetByID"),
		slog.String("match_id", id),
	)

	return "match#1", nil
}
