package services

import (
	"context"
	"football-tracker/internal/models"
	"football-tracker/internal/models/dto"
)

type MatchService interface {
	GetByChampionshipId(ctx context.Context, championshipId int) ([]dto.MatchDTO, error)
	GetById(ctx context.Context, matchId int) (dto.MatchDTO, error)
}

type ChampionshipService interface {
	GetAll(ctx context.Context) ([]models.Championship, error)
	GetByChampionshipId(ctx context.Context, championshipID int) (models.Championship, error)
}

type TeamService interface {
	GetByChampionshipId(ctx context.Context, championshipID int) ([]models.Team, error)
	GetById(ctx context.Context, championshipID int) (models.Team, error)
}

type AuthService interface {
	Register(ctx context.Context, req dto.RegisterRequest) (*dto.AuthResponse, error)
	Login(ctx context.Context, req dto.LoginRequest) (*dto.AuthResponse, error)
	Logout(ctx context.Context, userId int) error
	ValidateSession(ctx context.Context, token string) (*models.User, error)
}

type PredictionService interface {
	CreatePrediction(ctx context.Context, userID int, req *dto.CreatePredictionRequest) (*dto.PredictionResponse, error)
	// TOOD
	/*
		GetPredictionByID(ctx context.Context, predictionID int) (*dto.PredictionResponse, error)
		GetUserPredictions(ctx context.Context, userID int) (*dto.PredictionListResponse, error)
		GetMatchPredictions(ctx context.Context, matchID int) (*dto.PredictionListResponse, error)
		UpdatePrediction(ctx context.Context, userID, predictionID int, req *dto.UpdatePredictionRequest) (*dto.PredictionResponse, error)
		DeletePrediction(ctx context.Context, userID, predictionID int) error
		GetUserPredictionForMatch(ctx context.Context, userID, matchID int) (*dto.PredictionResponse, error)
	*/
}
