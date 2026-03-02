package repositories

import (
	"context"
	"database/sql"
	"fmt"
	"football-tracker/internal/models"
)

type predictionRepository struct {
	db *sql.DB
}

func NewPredictionRepository(db *sql.DB) PredictionRepository {
	return &predictionRepository{db: db}
}

// CreatePrediction creates a new prediction for a match
func (r *predictionRepository) CreatePrediction(ctx context.Context, prediction *models.Prediction) error {
	query := `
		INSERT INTO predictions (
			user_id, match_id, 
			home_score_regular, away_score_regular,
			home_score_total, away_score_total,
			penalty_winner_team_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at, updated_at
	`

	err := r.db.QueryRowContext(
		ctx, query,
		prediction.UserID,
		prediction.MatchID,
		prediction.HomeScoreRegular,
		prediction.AwayScoreRegular,
		prediction.HomeScoreTotal,
		prediction.AwayScoreTotal,
		prediction.PenaltyWinnerTeamID,
	).Scan(&prediction.ID, &prediction.CreatedAt, &prediction.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to create prediction: %w", err)
	}

	return nil
}

// GetPredictionByUserAndMatch retrieves a prediction for a specific user and match
func (r *predictionRepository) GetPredictionByUserAndMatch(ctx context.Context, userID, matchID int) (*models.Prediction, error) {
	query := `
		SELECT id, user_id, match_id,
		       home_score_regular, away_score_regular,
		       home_score_total, away_score_total,
		       penalty_winner_team_id,
		       points, created_at, updated_at
		FROM predictions
		WHERE user_id = $1 AND match_id = $2
	`

	prediction := &models.Prediction{}
	err := r.db.QueryRowContext(ctx, query, userID, matchID).Scan(
		&prediction.ID,
		&prediction.UserID,
		&prediction.MatchID,
		&prediction.HomeScoreRegular,
		&prediction.AwayScoreRegular,
		&prediction.HomeScoreTotal,
		&prediction.AwayScoreTotal,
		&prediction.PenaltyWinnerTeamID,
		&prediction.Points,
		&prediction.CreatedAt,
		&prediction.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil // No prediction found (not an error)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get prediction: %w", err)
	}

	return prediction, nil
}
