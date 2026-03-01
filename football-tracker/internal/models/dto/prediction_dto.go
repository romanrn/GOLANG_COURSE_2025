package dto

import "time"

// CreatePredictionRequest represents request to create a prediction for a match
type CreatePredictionRequest struct {
	MatchID int `json:"match_id" validate:"required,min=1" example:"1"`

	// Regular time prediction (required for all matches)
	HomeScoreRegular int `json:"home_score_regular" validate:"required,min=0" example:"2"`
	AwayScoreRegular int `json:"away_score_regular" validate:"required,min=0" example:"1"`

	// Total score prediction (optional, for playoff matches)
	HomeScoreTotal *int `json:"home_score_total,omitempty" validate:"omitempty,min=0" example:"3"`
	AwayScoreTotal *int `json:"away_score_total,omitempty" validate:"omitempty,min=0" example:"2"`

	// Penalty shootout winner prediction (optional, for playoff matches)
	PenaltyWinnerTeamID *int `json:"penalty_winner_team_id,omitempty" validate:"omitempty,min=1" example:"1"`
}

// PredictionResponse represents prediction data in response
type PredictionResponse struct {
	ID      int `json:"id" example:"1"`
	UserID  int `json:"user_id" example:"2"`
	MatchID int `json:"match_id" example:"1"`

	// Regular time prediction
	HomeScoreRegular int `json:"home_score_regular" example:"2"`
	AwayScoreRegular int `json:"away_score_regular" example:"1"`

	// Total score prediction (optional)
	HomeScoreTotal *int `json:"home_score_total,omitempty" example:"3"`
	AwayScoreTotal *int `json:"away_score_total,omitempty" example:"2"`

	// Penalty shootout winner prediction (optional)
	PenaltyWinnerTeamID *int `json:"penalty_winner_team_id,omitempty" example:"1"`

	Points    *int      `json:"points,omitempty" example:"3"`
	CreatedAt time.Time `json:"created_at" example:"2026-03-01T12:00:00Z"`
	UpdatedAt time.Time `json:"updated_at" example:"2026-03-01T12:00:00Z"`
}
