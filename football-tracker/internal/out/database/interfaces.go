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

type UserRepository interface {
	Create(ctx context.Context, user *models.User) error
	GetByUsername(ctx context.Context, username string) (*models.User, error)
	GetByEmail(ctx context.Context, email string) (*models.User, error)
	GetById(ctx context.Context, userId int) (*models.User, error)
}

type SessionRepository interface {
	Create(ctx context.Context, session *models.Session) error
	GetByToken(ctx context.Context, token string) (*models.Session, error)
	DeleteByToken(ctx context.Context, token string) error
	DeleteByUserId(ctx context.Context, userId int) error
	DeleteExpired(ctx context.Context) error
}

type PredictionRepository interface {
	CreatePrediction(ctx context.Context, prediction *models.Prediction) error
	GetPredictionByUserAndMatch(ctx context.Context, userID, matchID int) (*models.Prediction, error)
	// TODO
	/*
		GetPredictionByID(ctx context.Context, predictionID int) (*models.Prediction, error)
		GetPredictionByUserAndMatch(ctx context.Context, userID, matchID int) (*models.Prediction, error)
		GetPredictionsByUser(ctx context.Context, userID int) ([]models.Prediction, error)
		GetPredictionsByMatch(ctx context.Context, matchID int) ([]models.Prediction, error)
		UpdatePrediction(ctx context.Context, prediction *models.Prediction) error
		DeletePrediction(ctx context.Context, predictionID, userID int) error

	*/
}
