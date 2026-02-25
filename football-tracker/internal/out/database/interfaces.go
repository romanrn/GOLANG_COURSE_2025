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
	GetByChampionshipId(ctx context.Context, championshipId int) (models.Championship, error)
}

type TeamRepository interface {
	GetByChampionshipId(ctx context.Context, championshipId int) ([]models.Team, error)
	GetById(ctx context.Context, teamId int) (models.Team, error)
}
