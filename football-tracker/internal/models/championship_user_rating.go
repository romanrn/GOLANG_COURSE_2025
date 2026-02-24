package models

import "time"

// ChampionshipUserRating represents user statistics for a specific championship
type ChampionshipUserRating struct {
	ID             int `json:"id" db:"id"`
	UserID         int `json:"user_id" db:"user_id"`
	ChampionshipID int `json:"championship_id" db:"championship_id"`

	// Statistics
	TotalPoints        int     `json:"total_points" db:"total_points"`
	TotalPredictions   int     `json:"total_predictions" db:"total_predictions"`
	CorrectScores      int     `json:"correct_scores" db:"correct_scores"`     // Exact score predictions
	CorrectOutcomes    int     `json:"correct_outcomes" db:"correct_outcomes"` // Correct outcome predictions
	WrongPredictions   int     `json:"wrong_predictions" db:"wrong_predictions"`
	AccuracyPercentage float64 `json:"accuracy_percentage" db:"accuracy_percentage"`
	AveragePoints      float64 `json:"average_points" db:"average_points"`
	Rank               *int    `json:"rank,omitempty" db:"rank"` // User's rank in this championship

	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// TableName returns the table name for ChampionshipUserRating
func (ChampionshipUserRating) TableName() string {
	return "championship_user_ratings"
}

// UpdateStats recalculates statistics based on predictions
func (r *ChampionshipUserRating) UpdateStats() {
	if r.TotalPredictions > 0 {
		r.AccuracyPercentage = float64(r.CorrectScores+r.CorrectOutcomes) / float64(r.TotalPredictions) * 100
		r.AveragePoints = float64(r.TotalPoints) / float64(r.TotalPredictions)
	} else {
		r.AccuracyPercentage = 0
		r.AveragePoints = 0
	}
}
