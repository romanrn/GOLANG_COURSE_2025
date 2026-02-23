package services

import (
	"context"
	repositories "football-tracker/internal/out/database"
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
	_, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return "", err
	}
	return "match#1", nil
}
