package repositories

import (
	"context"
	"football-tracker/internal/models"
)

type MatchRepository interface {
	GetByID(ctx context.Context, id string) (string, error)
}

type ChampionshipRepository interface {
	GetAll(ctx context.Context) ([]models.Championship, error)
}
