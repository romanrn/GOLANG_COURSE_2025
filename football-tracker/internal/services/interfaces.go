package services

import (
	"context"
	"football-tracker/internal/models"
)

type MatchService interface {
	GetById(ctx context.Context, id string) (string, error)
}

type ChampionshipService interface {
	GetAll(ctx context.Context) ([]models.Championship, error)
	GetByChampionshipId(ctx context.Context, championshipID int) (models.Championship, error)
}

type TeamService interface {
	GetByChampionshipId(ctx context.Context, championshipID int) ([]models.Team, error)
	GetById(ctx context.Context, championshipID int) (models.Team, error)
}
