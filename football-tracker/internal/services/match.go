package services

import (
	"context"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/models/dto"
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

func (s *matchService) GetByChampionshipId(ctx context.Context, championshipId int) ([]dto.MatchDTO, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting matches for championship",
		slog.String("component", "service"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
	)

	matches, err := s.repo.GetByChampionshipId(ctx, championshipId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get matches from repository",
			slog.String("component", "service"),
			slog.String("method", "GetByChampionshipId"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to get matches for championship: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved matches for championship",
		slog.String("component", "service"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
		slog.Int("count", len(matches)),
	)

	return matches, nil
}

func (s *matchService) GetById(ctx context.Context, matchId int) (dto.MatchDTO, error) {
	logger.GetLogger().Info(
		ctx,
		"Getting match by ID",
		slog.String("component", "service"),
		slog.String("method", "GetById"),
		slog.Int("match_id", matchId),
	)

	match, err := s.repo.GetById(ctx, matchId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get match by ID",
			slog.String("component", "service"),
			slog.String("method", "GetById"),
			slog.Int("match_id", matchId),
			slog.String("error", err.Error()),
		)
		return dto.MatchDTO{}, fmt.Errorf("failed to get match by ID: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved match by ID",
		slog.String("component", "service"),
		slog.String("method", "GetById"),
		slog.Int("match_id", matchId),
		slog.String("status", match.Status),
		slog.String("stage", match.Stage),
	)

	return match, nil
}
