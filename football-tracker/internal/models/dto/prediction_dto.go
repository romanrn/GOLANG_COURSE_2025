package dto

import "time"

// CreatePredictionRequest represents request to create a prediction for a match
type CreatePredictionRequest struct {
	MatchID int `json:"match_id" validate:"required,min=1"`

	// Regular time prediction (required for all matches)
	HomeScoreRegular int `json:"home_score_regular" validate:"required,min=0"`
	AwayScoreRegular int `json:"away_score_regular" validate:"required,min=0"`

	// Total score prediction (optional, for playoff matches)
	HomeScoreTotal *int `json:"home_score_total,omitempty" validate:"omitempty,min=0"`
	AwayScoreTotal *int `json:"away_score_total,omitempty" validate:"omitempty,min=0"`

	// Penalty shootout winner prediction (optional, for playoff matches)
	PenaltyWinnerTeamID *int `json:"penalty_winner_team_id,omitempty" validate:"omitempty,min=1"`
}

// PredictionResponse represents prediction data in response
type PredictionResponse struct {
	ID      int `json:"id"`
	UserID  int `json:"user_id"`
	MatchID int `json:"match_id"`

	// Regular time prediction
	HomeScoreRegular int `json:"home_score_regular"`
	AwayScoreRegular int `json:"away_score_regular"`

	// Total score prediction (optional)
	HomeScoreTotal *int `json:"home_score_total,omitempty"`
	AwayScoreTotal *int `json:"away_score_total,omitempty"`

	// Penalty shootout winner prediction (optional)
	PenaltyWinnerTeamID *int `json:"penalty_winner_team_id,omitempty"`

	Points    *int      `json:"points,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
