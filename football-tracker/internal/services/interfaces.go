package services

import (
	"context"
	"football-tracker/internal/models"
)

type MatchService interface {
	GetByID(ctx context.Context, id string) (string, error)
}

type ChampionshipService interface {
	GetAll(ctx context.Context) ([]models.Championship, error)
}
