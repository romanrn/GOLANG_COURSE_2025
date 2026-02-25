package repositories

import (
	"context"
	"football-tracker/internal/models"
	"football-tracker/internal/models/dto"
)

type MatchRepository interface {
	GetByChampionshipId(ctx context.Context, championshipId int) ([]dto.MatchDTO, error)
	GetById(ctx context.Context, matchId int) (dto.MatchDTO, error)
}

type ChampionshipRepository interface {
	GetAll(ctx context.Context) ([]models.Championship, error)
	GetByChampionshipId(ctx context.Context, championshipId int) (models.Championship, error)
}

type TeamRepository interface {
	GetByChampionshipId(ctx context.Context, championshipId int) ([]models.Team, error)
	GetById(ctx context.Context, teamId int) (models.Team, error)
}
