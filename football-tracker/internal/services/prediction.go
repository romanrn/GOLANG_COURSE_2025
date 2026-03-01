package services

import (
	"context"
	"fmt"
	"football-tracker/internal/models"
	"football-tracker/internal/models/dto"
	repositories "football-tracker/internal/out/database"
	"time"
)

type predictionService struct {
	predictionRepo repositories.PredictionRepository
	matchRepo      repositories.MatchRepository
}

func NewPredictionService(predictionRepo repositories.PredictionRepository, matchRepo repositories.MatchRepository) PredictionService {
	return &predictionService{
		predictionRepo: predictionRepo,
		matchRepo:      matchRepo,
	}
}

// CreatePrediction creates a new prediction for a match
func (s *predictionService) CreatePrediction(ctx context.Context, userID int, req *dto.CreatePredictionRequest) (*dto.PredictionResponse, error) {
	// Validate that match exists and is not yet started
	match, err := s.matchRepo.GetById(ctx, req.MatchID)
	if err != nil {
		return nil, fmt.Errorf("failed to get match: %w", err)
	}

	// Check if match has already started
	if match.MatchDate.Before(time.Now()) {
		return nil, fmt.Errorf("cannot create prediction for a match that has already started")
	}

	// Check if user already has a prediction for this match
	existing, err := s.predictionRepo.GetPredictionByUserAndMatch(ctx, userID, req.MatchID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing prediction: %w", err)
	}
	if existing != nil {
		return nil, fmt.Errorf("prediction for this match already exists")
	}

	// Validate total scores if provided
	if (req.HomeScoreTotal != nil && req.AwayScoreTotal == nil) ||
		(req.HomeScoreTotal == nil && req.AwayScoreTotal != nil) {
		return nil, fmt.Errorf("both home_score_total and away_score_total must be provided together")
	}

	// Create prediction
	prediction := &models.Prediction{
		UserID:              userID,
		MatchID:             req.MatchID,
		HomeScoreRegular:    req.HomeScoreRegular,
		AwayScoreRegular:    req.AwayScoreRegular,
		HomeScoreTotal:      req.HomeScoreTotal,
		AwayScoreTotal:      req.AwayScoreTotal,
		PenaltyWinnerTeamID: req.PenaltyWinnerTeamID,
	}

	err = s.predictionRepo.CreatePrediction(ctx, prediction)
	if err != nil {
		return nil, fmt.Errorf("failed to create prediction: %w", err)
	}

	return s.mapToResponse(prediction), nil
}

// mapToResponse converts Prediction model to PredictionResponse DTO
func (s *predictionService) mapToResponse(prediction *models.Prediction) *dto.PredictionResponse {
	return &dto.PredictionResponse{
		ID:                  prediction.ID,
		UserID:              prediction.UserID,
		MatchID:             prediction.MatchID,
		HomeScoreRegular:    prediction.HomeScoreRegular,
		AwayScoreRegular:    prediction.AwayScoreRegular,
		HomeScoreTotal:      prediction.HomeScoreTotal,
		AwayScoreTotal:      prediction.AwayScoreTotal,
		PenaltyWinnerTeamID: prediction.PenaltyWinnerTeamID,
		Points:              prediction.Points,
		CreatedAt:           prediction.CreatedAt,
		UpdatedAt:           prediction.UpdatedAt,
	}
}
