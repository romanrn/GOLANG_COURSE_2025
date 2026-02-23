package services

import (
	"context"
)

type matchService struct{}

func NewMatchService() MatchService {
	return &matchService{}
}

func (s *matchService) GetByID(ctx context.Context, id string) (string, error) {
	return "match#1", nil
}
